use crate::page_handle::PageKey;
use crate::prelude::*;
use crate::{GlobalAllocExt, PageCache, PageCacheFlusher, PageHandle, PageState};
use block_device::{BlockDevice, BlockDeviceExt, BlockId, BLOCK_SIZE};

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

/// A virtual disk with a backing disk and a page cache.
///
/// Thanks to the page cache, accessing a disk through
/// `CachedDisk` is expected to be faster than
/// accessing the disk directly.
///
/// `CachedDisk` exhibits the write-back strategy: writes
/// are first cached in memory, and later flushed to the
/// backing disk. The flush is either triggered by an
/// explicit flush operation or performed by a background
/// flusher task.
///
/// The memory allocator for the page cache is specified
/// by the generic parameter `A` of `CachedDisk<A>`.
pub struct CachedDisk<A: GlobalAllocExt>(Arc<Inner<A>>);

impl PageKey for BlockId {}

struct Inner<A: GlobalAllocExt> {
    disk: Box<dyn BlockDevice>,
    cache: PageCache<BlockId, A>,
    flusher_wq: WaiterQueue,
    // This read-write lock is used to control the concurrent
    // writers and flushers. A writer acquires the read lock,
    // permitting multiple writers, but no flusher. A flusher
    // acquires the write lock to forbid any other flushers
    // and writers. This policy is important to implement
    // the semantic of the flush operation correctly.
    // TODO: Async lock
    // arw_lock: AsyncRwLock,
    rw_lock: RwLock<usize>,
    // Whether CachedDisk is droppedd
    is_dropped: AtomicBool,
}

impl<A: GlobalAllocExt> CachedDisk<A> {
    /// Create a new `CachedDisk`.
    pub fn new(disk: Box<dyn BlockDevice>) -> Result<Self> {
        let flusher = Arc::new(CachedDiskFlusher::<A>::new());
        let cache = PageCache::new(0x1000, flusher.clone());
        let flusher_wq = WaiterQueue::new();
        let arc_inner = Arc::new(Inner {
            disk,
            cache,
            flusher_wq,
            // arw_lock: AsyncRwLock::new(),
            rw_lock: RwLock::new(0),
            is_dropped: AtomicBool::new(false),
        });
        let new_self = Self(arc_inner);

        flusher.set_disk(new_self.0.clone());
        new_self.spawn_flusher_task();

        Ok(new_self)
    }

    /// Spawn a flusher task.
    ///
    /// The task flusher dirty pages in the page cache periodically.
    /// This flusher is not to be confused with `PageCacheFlusher`,
    /// the latter of which flushes dirty pages and evict pages to
    /// release memory when the free memory is low.
    fn spawn_flusher_task(&self) {
        // Spawn the flusher task
        const AUTO_FLUSH_PERIOD: Duration = Duration::from_secs(5);
        let this = self.0.clone();
        async_rt::task::spawn(async move {
            let mut waiter = Waiter::new();
            this.flusher_wq.enqueue(&mut waiter);
            loop {
                // If CachedDisk is dropped, then the flusher task should exit
                if this.is_dropped.load(Ordering::Relaxed) {
                    break;
                }

                // Wait until being notified or timeout
                let mut timeout = AUTO_FLUSH_PERIOD;
                let _ = waiter.wait_timeout(Some(&mut timeout)).await;

                // Do flush
                let _ = this.flush().await;
            }
            this.flusher_wq.dequeue(&mut waiter);
        });
    }

    /// Read blocks starting from `from_bid` into the given buffer.
    ///
    /// The length of buffer must be a multiple of BLOCK_SIZE. On
    /// success, the number of bytes read is returned.
    pub async fn read(&self, from_bid: BlockId, buf: &mut [u8]) -> Result<usize> {
        self.0.read(from_bid, buf).await
    }

    /// Read blocks starting from `from_bid` into the given buffer.
    ///
    /// The length of buffer must be a multiple of BLOCK_SIZE. On
    /// success, the number of bytes written in returned.
    pub async fn write(&self, from_bid: BlockId, buf: &[u8]) -> Result<usize> {
        self.0.write(from_bid, buf).await
    }

    /// Flush all changes onto the backing disk.
    pub async fn flush(&self) -> Result<usize> {
        self.0.flush().await
    }
}

impl<A: GlobalAllocExt> Drop for CachedDisk<A> {
    fn drop(&mut self) {
        self.0.is_dropped.store(true, Ordering::Relaxed);
        self.0.flusher_wq.wake_all();
    }
}

impl<A: GlobalAllocExt> Inner<A> {
    pub async fn read(&self, from_bid: BlockId, buf: &mut [u8]) -> Result<usize> {
        let num_blocks = self.check_rw_args(from_bid, buf)?;

        for (i, this_bid) in (from_bid..from_bid + num_blocks).enumerate() {
            let page_handle = self.acquire_page(this_bid).await?;
            let mut page_guard = page_handle.lock();

            // Ensure the page is ready for read
            loop {
                match page_guard.state() {
                    // The page is ready for read
                    PageState::UpToDate | PageState::Dirty | PageState::Flushing => {
                        drop(page_guard);
                        break;
                    }
                    // The page is not initialized. So we need to
                    // read it from the disk.
                    PageState::Uninit => {
                        page_guard.set_state(PageState::Fetching);
                        Self::clear_page_events(&page_handle);
                        let page_ptr = page_guard.page().as_slice_mut();

                        let page_slice = unsafe {
                            std::slice::from_raw_parts_mut(page_ptr.as_mut_ptr(), BLOCK_SIZE)
                        };
                        drop(page_guard);
                        // TODO: handle error
                        self.read_page(this_bid, page_slice).await.unwrap();
                        Self::notify_page_events(&page_handle, Events::IN);

                        let mut page_guard = page_handle.lock();
                        debug_assert!(page_guard.state() == PageState::Fetching);
                        page_guard.set_state(PageState::UpToDate);
                        break;
                    }
                    // The page is being fetched. We just try again
                    // later to see if it is ready.
                    PageState::Fetching => {
                        drop(page_guard);

                        Self::wait_page_events(&page_handle, Events::IN).await;

                        page_guard = page_handle.lock();
                    }
                }
            }
            let mut page_guard = page_handle.lock();
            let dst_buf = &mut buf[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE];
            let src_buf = page_guard.page().as_slice();
            dst_buf.copy_from_slice(src_buf);

            drop(page_guard);
            self.cache.release(&page_handle);
        }
        Ok(num_blocks * BLOCK_SIZE)
    }

    pub async fn write(&self, from_bid: BlockId, buf: &[u8]) -> Result<usize> {
        let num_blocks = self.check_rw_args(from_bid, buf)?;

        // let _ = self.arw_lock.read().await;
        let sem = self.rw_lock.read();

        for (i, this_bid) in (from_bid..from_bid + num_blocks).enumerate() {
            let page_handle = self.acquire_page(this_bid).await?;
            let mut page_guard = page_handle.lock();

            // Ensure the page is ready for write
            loop {
                match page_guard.state() {
                    // The page is ready for write
                    PageState::UpToDate | PageState::Dirty | PageState::Uninit => {
                        break;
                    }
                    // The page is being fetched. We just try again
                    // later to see if it is ready.
                    PageState::Fetching | PageState::Flushing => {
                        drop(page_guard);

                        Self::wait_page_events(&page_handle, Events::IN | Events::OUT).await;

                        page_guard = page_handle.lock();
                    }
                }
            }

            let dst_buf = page_guard.page().as_slice_mut();
            let src_buf = &buf[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE];
            dst_buf.copy_from_slice(src_buf);
            page_guard.set_state(PageState::Dirty);

            drop(page_guard);
            self.cache.release(&page_handle);
        }
        drop(sem);
        Ok(num_blocks * BLOCK_SIZE)
    }

    pub async fn flush(&self) -> Result<usize> {
        let mut total_pages = 0;

        // let _ = self.arw_lock().write().await;
        let sem = self.rw_lock.write();

        const BATCH_SIZE: usize = 128;
        let mut flush_pages = Vec::with_capacity(BATCH_SIZE);
        loop {
            let num_pages = self.cache.flush_dirty(&mut flush_pages);
            if num_pages == 0 {
                break;
            }

            for page_handle in &flush_pages {
                let mut page_guard = page_handle.lock();
                debug_assert!(page_guard.state() == PageState::Flushing);
                Self::clear_page_events(&page_handle);

                let block_id = page_handle.key();
                let page_ptr = page_guard.page().as_slice();

                let page_buf = unsafe { std::slice::from_raw_parts(page_ptr.as_ptr(), BLOCK_SIZE) };
                drop(page_guard);
                // TODO: handle error
                self.write_page(&block_id, page_buf).await.unwrap();
                Self::notify_page_events(&page_handle, Events::OUT);

                let mut page_guard = page_handle.lock();
                page_guard.set_state(PageState::UpToDate);
                drop(page_guard);

                self.cache.release(page_handle);
            }

            total_pages += num_pages;
        }

        self.disk.flush().await?;
        drop(sem);
        // At this point, we can be certain that all writes
        // have been persisted to the disk because
        // 1) There are no concurrent writers;
        // 2) There are no concurrent flushers;
        // 3) All dirty pages have been cleared;
        // 4) The underlying disk is also flushed.
        Ok(total_pages)
    }

    /// Check if the arguments for a read or write is valid. If so,
    /// return the number of blocks to read or write. Otherwise,
    /// return an error.
    fn check_rw_args(&self, from_bid: BlockId, buf: &[u8]) -> Result<usize> {
        debug_assert!(buf.len() % BLOCK_SIZE == 0);

        if from_bid >= self.disk.total_blocks() {
            return Ok(0);
        }

        let max_blocks = self.disk.total_blocks() - from_bid;
        let num_blocks = buf.len() / BLOCK_SIZE;
        Ok(num_blocks.min(max_blocks))
    }

    async fn acquire_page(&self, block_id: BlockId) -> Result<PageHandle<BlockId, A>> {
        loop {
            if let Some(page_handle) = self.cache.acquire(block_id) {
                break Ok(page_handle);
            }

            let mut poller = Poller::new();
            let events = self.cache.poll(Some(&mut poller));
            if !events.is_empty() {
                continue;
            }

            poller.wait().await;
        }
    }

    async fn read_page(&self, block_id: BlockId, buf: &mut [u8]) -> Result<usize> {
        let offset = block_id * BLOCK_SIZE;
        self.disk.read(offset, buf).await
    }

    async fn write_page(&self, block_id: &BlockId, buf: &[u8]) -> Result<usize> {
        let offset = block_id * BLOCK_SIZE;
        self.disk.write(offset, buf).await
    }

    fn clear_page_events(page_handle: &PageHandle<BlockId, A>) {
        page_handle.pollee().reset_events();
    }

    fn notify_page_events(page_handle: &PageHandle<BlockId, A>, events: Events) {
        page_handle.pollee().add_events(events)
    }

    async fn wait_page_events(page_handle: &PageHandle<BlockId, A>, events: Events) {
        let mut poller = Poller::new();
        if page_handle
            .pollee()
            .poll(events, Some(&mut poller))
            .is_empty()
        {
            poller.wait().await;
        }
    }
}

struct CachedDiskFlusher<A: GlobalAllocExt> {
    // this_opt => CachedDisk
    this_opt: Mutex<Option<Arc<Inner<A>>>>,
}

pub type Result<T> = core::result::Result<T, Error>;
impl<A: GlobalAllocExt> CachedDiskFlusher<A> {
    pub fn new() -> Self {
        Self {
            this_opt: Mutex::new(None),
        }
    }

    pub fn set_disk(&self, this: Arc<Inner<A>>) {
        *self.this_opt.lock() = Some(this);
    }

    fn this_opt(&self) -> Option<Arc<Inner<A>>> {
        self.this_opt.lock().clone()
    }
}

#[async_trait]
impl<A: GlobalAllocExt> PageCacheFlusher for CachedDiskFlusher<A> {
    async fn flush(&self) -> Result<usize> {
        if let Some(this) = self.this_opt() {
            this.flush().await?;
        }
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global_alloc_ext::FixedSizeAlloc;
    use block_device::mem_disk::MemDisk;

    fn new_cached_disk() -> CachedDisk<FixedSizeAlloc> {
        const TOTAL_BLOCKS: usize = 0x1000;
        let mem_disk = MemDisk::new(TOTAL_BLOCKS).unwrap();
        CachedDisk::<FixedSizeAlloc>::new(Box::new(mem_disk)).unwrap()
    }

    #[test]
    fn cached_disk_write_read() -> Result<()> {
        async_rt::task::block_on(async move {
            let cached_disk = new_cached_disk();
            let block_id: BlockId = 125;
            let content: u8 = 5;
            const SIZE: usize = 4096;

            let mut read_buf: [u8; SIZE] = [0; SIZE];
            let len = cached_disk.read(block_id, &mut read_buf[..]).await?;
            assert_eq!(SIZE, len, "[CachedDisk] read failed");

            let write_buf: [u8; SIZE] = [content; SIZE];
            let len = cached_disk.write(block_id, &write_buf[..]).await?;
            assert_eq!(SIZE, len, "[CachedDisk] write failed");

            let len = cached_disk.read(block_id, &mut read_buf[..]).await?;
            assert_eq!(SIZE, len, "[CachedDisk] read failed");
            assert_eq!(read_buf, write_buf, "[CachedDisk] read wrong content");
            Ok(())
        })
    }

    #[test]
    fn cached_disk_flush() -> Result<()> {
        async_rt::task::block_on(async move {
            let cached_disk = new_cached_disk();
            const SIZE: usize = 4096;
            let write_cnt = 100;
            for block_id in 0..write_cnt {
                let write_buf: [u8; SIZE] = [0; SIZE];
                let len = cached_disk.write(block_id, &write_buf[..]).await?;
                assert_eq!(SIZE, len, "[CachedDisk] write failed");
            }
            let flush_num = cached_disk.flush().await?;
            assert_eq!(flush_num, write_cnt, "[CachedDisk] flush failed");

            let write_cnt = 1000;
            for block_id in 0..write_cnt {
                let write_buf: [u8; SIZE] = [0; SIZE];
                let len = cached_disk.write(block_id, &write_buf[..]).await?;
                assert_eq!(SIZE, len, "[CachedDisk] write failed");
            }
            let flush_num = cached_disk.flush().await?;
            const FLUSH_BATCH_SIZE: usize = 128;
            assert_eq!(flush_num, FLUSH_BATCH_SIZE, "[CachedDisk] flush failed");
            Ok(())
        })
    }

    #[test]
    fn cached_disk_rwf() -> Result<()> {
        async_rt::task::block_on(async move {
            let cached_disk = Arc::new(new_cached_disk());
            let reader = cached_disk.clone();
            let writer = cached_disk.clone();
            let flusher = cached_disk.clone();
            const SIZE: usize = 4096;
            let rwf_cnt = 1000;

            let reader_handle = async_rt::task::spawn(async move {
                for block_id in 0..rwf_cnt {
                    let mut read_buf: [u8; SIZE] = [0; SIZE];
                    reader.read(block_id, &mut read_buf[..]).await.unwrap();
                }
            });
            let writer_handle = async_rt::task::spawn(async move {
                for block_id in 0..rwf_cnt {
                    let write_buf: [u8; SIZE] = [0; SIZE];
                    writer.write(block_id, &write_buf[..]).await.unwrap();
                }
            });
            let flusher_handle = async_rt::task::spawn(async move {
                for _ in 0..rwf_cnt {
                    flusher.flush().await.unwrap();
                }
            });

            reader_handle.await;
            writer_handle.await;
            flusher_handle.await;
            Ok(())
        })
    }
}
