use crate::prelude::*;
use block_device::{
    BioReqBuilder, BioType, BlockBuf, BlockDevice, BlockDeviceExt, BlockId, BLOCK_SIZE,
};

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
pub struct CachedDisk<A: PageAlloc>(Arc<Inner<A>>);

impl PageKey for BlockId {}

struct Inner<A: PageAlloc> {
    disk: Arc<dyn BlockDevice>,
    cache: PageCache<BlockId, A>,
    flusher_wq: WaiterQueue,
    // This read-write lock is used to control the concurrent
    // writers and flushers. A writer acquires the read lock,
    // permitting multiple writers, but no flusher. A flusher
    // acquires the write lock to forbid any other flushers
    // and writers. This policy is important to implement
    // the semantic of the flush operation correctly.
    arw_lock: AsyncRwLock<usize>,
    // Whether CachedDisk is droppedd
    is_dropped: AtomicBool,
}

impl<A: PageAlloc> CachedDisk<A> {
    /// Create a new `CachedDisk`.
    pub fn new(disk: Arc<dyn BlockDevice>) -> Result<Self> {
        let flusher = Arc::new(CachedDiskFlusher::<A>::new());
        let cache = PageCache::new(flusher.clone());
        let flusher_wq = WaiterQueue::new();
        let arc_inner = Arc::new(Inner {
            disk,
            cache,
            flusher_wq,
            arw_lock: AsyncRwLock::new(0),
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

    /// Read cache content from `offset` into the given buffer.
    ///
    /// The length of buffer and offset can be arbitrary.
    /// On success, the number of bytes read is returned.
    pub async fn read(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.0.read(offset, buf).await
    }

    /// Write buffer content into cache starting from `offset`.
    ///
    /// The length of buffer and offset can be arbitrary.
    /// On success, the number of bytes written in returned.
    pub async fn write(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        self.0.write(offset, buf).await
    }

    /// Flush all changes onto the backing disk.
    pub async fn flush(&self) -> Result<usize> {
        self.0.flush_disk().await
    }
}

impl<A: PageAlloc> Drop for CachedDisk<A> {
    fn drop(&mut self) {
        self.0.is_dropped.store(true, Ordering::Relaxed);
        self.0.flusher_wq.wake_all();
    }
}

/// BlockRange represents block information
/// of one read/write operation.
struct BlockRange {
    // id of first block
    begin_id: BlockId,
    // offset of first block
    begin_offset: usize,
    // id of last block
    end_id: BlockId,
    // offset of last block
    end_offset: usize,
}

impl BlockRange {
    fn from(offset: usize, buf: &[u8]) -> Self {
        let begin_id = offset / BLOCK_SIZE;
        let begin_offset = offset % BLOCK_SIZE;
        let end_id = (offset + buf.len()) / BLOCK_SIZE;
        let end_offset = (offset + buf.len()) % BLOCK_SIZE;
        Self {
            begin_id,
            begin_offset,
            end_id,
            end_offset,
        }
    }
}

impl<A: PageAlloc> Inner<A> {
    pub async fn read(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        let block_range = self.check_rw_args(offset, buf);

        let mut buf_pos = 0;
        let mut read_len = 0;
        // Read one partial/whole page each time
        for current_id in block_range.begin_id..=block_range.end_id {
            let mut begin_pos = block_range.begin_offset;
            let mut end_pos = block_range.end_offset;
            if current_id != block_range.begin_id {
                begin_pos = 0;
            }
            if current_id != block_range.end_id {
                end_pos = BLOCK_SIZE;
            }

            let read_buf = &mut buf[buf_pos..buf_pos + end_pos - begin_pos];
            if read_buf.len() > 0 {
                read_len +=
                    if let Ok(len) = self.read_one_page(current_id, read_buf, begin_pos).await {
                        len
                    } else {
                        error!("[CachedDisk] read one page failed, id: {}", current_id);
                        0
                    }
            }

            buf_pos += end_pos - begin_pos;
        }

        debug_assert!(read_len == buf.len());
        Ok(read_len)
    }

    pub async fn write(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        let block_range = self.check_rw_args(offset, buf);

        let mut buf_pos = 0;
        let mut write_len = 0;
        // Write one partial/whole page each time
        for current_id in block_range.begin_id..=block_range.end_id {
            let mut begin_pos = block_range.begin_offset;
            let mut end_pos = block_range.end_offset;
            if current_id != block_range.begin_id {
                begin_pos = 0;
            }
            if current_id != block_range.end_id {
                end_pos = BLOCK_SIZE;
            }

            let write_buf = &buf[buf_pos..buf_pos + end_pos - begin_pos];
            if write_buf.len() > 0 {
                write_len +=
                    if let Ok(len) = self.write_one_page(current_id, write_buf, begin_pos).await {
                        len
                    } else {
                        error!("[CachedDisk] write one page failed, id: {}", current_id);
                        0
                    }
            }

            buf_pos += end_pos - begin_pos;
        }

        debug_assert!(write_len == buf.len());
        Ok(write_len)
    }

    /// Check if the arguments for a read or write is valid.
    /// If so, return the formed BlockRange.
    /// Otherwise, return an error.
    fn check_rw_args(&self, offset: usize, buf: &[u8]) -> BlockRange {
        debug_assert!(offset + buf.len() <= self.disk.total_blocks() * BLOCK_SIZE);
        let block_range = BlockRange::from(offset, buf);
        block_range
    }

    async fn read_one_page(&self, bid: BlockId, buf: &mut [u8], offset: usize) -> Result<usize> {
        debug_assert!(buf.len() + offset <= BLOCK_SIZE);

        let page_handle = self.acquire_page(bid).await?;
        let mut page_guard = page_handle.lock();

        // Ensure the page is ready for read
        loop {
            match page_guard.state() {
                // The page is ready for read
                PageState::UpToDate | PageState::Dirty | PageState::Flushing => {
                    break;
                }
                // The page is not initialized. So we need to
                // read it from the disk.
                PageState::Uninit => {
                    page_guard.set_state(PageState::Fetching);
                    Self::clear_page_events(&page_handle);

                    let page_ptr = page_guard.as_slice_mut();
                    let page_buf = unsafe {
                        std::slice::from_raw_parts_mut(page_ptr.as_mut_ptr(), BLOCK_SIZE)
                    };
                    drop(page_guard);

                    // Read one block from disk to current page
                    self.read_block(bid, page_buf).await?;
                    Self::notify_page_events(&page_handle, Events::IN);

                    page_guard = page_handle.lock();
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

        let read_len = buf.len();
        let src_buf = page_guard.as_slice();
        buf.copy_from_slice(&src_buf[offset..offset + read_len]);

        drop(page_guard);
        self.cache.release(page_handle);
        Ok(read_len)
    }

    async fn write_one_page(&self, bid: BlockId, buf: &[u8], offset: usize) -> Result<usize> {
        debug_assert!(buf.len() + offset <= BLOCK_SIZE);
        let sem = self.arw_lock.read().await;

        let page_handle = self.acquire_page(bid).await?;
        let mut page_guard = page_handle.lock();

        // Ensure the page is ready for write
        loop {
            match page_guard.state() {
                PageState::Uninit => {
                    // Read latest content of current page from disk before write.
                    // Only occur in partial writes.
                    if buf.len() < BLOCK_SIZE {
                        page_guard.set_state(PageState::Fetching);
                        Self::clear_page_events(&page_handle);

                        let page_ptr = page_guard.as_slice_mut();
                        let page_buf = unsafe {
                            std::slice::from_raw_parts_mut(page_ptr.as_mut_ptr(), BLOCK_SIZE)
                        };
                        drop(page_guard);

                        self.read_block(bid, page_buf).await?;
                        Self::notify_page_events(&page_handle, Events::IN);

                        page_guard = page_handle.lock();
                        debug_assert!(page_guard.state() == PageState::Fetching);
                        page_guard.set_state(PageState::UpToDate);
                    }
                    break;
                }
                // The page is ready for write
                PageState::UpToDate | PageState::Dirty => {
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

        let write_len = buf.len();
        let dst_buf = page_guard.as_slice_mut();
        dst_buf[offset..offset + write_len].copy_from_slice(buf);
        page_guard.set_state(PageState::Dirty);

        drop(page_guard);
        self.cache.release(page_handle);
        drop(sem);
        Ok(write_len)
    }

    pub(crate) async fn flush(&self) -> Result<usize> {
        let mut total_pages = 0;
        let sem = self.arw_lock.write().await;

        let mut flush_pages = Vec::with_capacity(128);
        loop {
            flush_pages.clear();
            let num_pages = self.cache.flush_dirty(&mut flush_pages);
            if num_pages == 0 {
                break;
            }

            for page_handle in &flush_pages {
                let page_guard = page_handle.lock();
                debug_assert!(page_guard.state() == PageState::Flushing);
                Self::clear_page_events(&page_handle);

                let bid = page_handle.key();
                let page_ptr = page_guard.as_slice();
                let page_buf = unsafe { std::slice::from_raw_parts(page_ptr.as_ptr(), BLOCK_SIZE) };
                drop(page_guard);

                self.write_block(&bid, page_buf).await.unwrap();
                Self::notify_page_events(&page_handle, Events::OUT);

                let mut page_guard = page_handle.lock();
                debug_assert!(page_guard.state() == PageState::Flushing);
                page_guard.set_state(PageState::UpToDate);
                drop(page_guard);

                self.cache.release(page_handle.clone());
            }

            total_pages += num_pages;
        }

        drop(sem);
        // At this point, we can be certain that all writes
        // have been persisted to the disk because
        // 1) There are no concurrent writers;
        // 2) There are no concurrent flushers;
        // 3) All dirty pages have been cleared;
        // 4) The underlying disk is also flushed.
        trace!("[CachedDisk] flush pages: {}", total_pages);
        Ok(total_pages)
    }

    pub(crate) async fn flush_disk(&self) -> Result<usize> {
        let flush_num = self.flush().await.unwrap();
        self.disk.flush().await;
        Ok(flush_num)
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

    async fn read_block(&self, block_id: BlockId, buf: &mut [u8]) -> Result<usize> {
        let offset = block_id * BLOCK_SIZE;
        self.disk.read(offset, buf).await
    }

    async fn write_block(&self, block_id: &BlockId, buf: &[u8]) -> Result<usize> {
        let offset = block_id * BLOCK_SIZE;
        self.disk.write(offset, buf).await
    }

    async fn batch_write_blocks(&self, addr: usize, write_bufs: Vec<BlockBuf>) -> Result<()> {
        let req = BioReqBuilder::new(BioType::Write)
            .addr(addr)
            .bufs(write_bufs)
            .build();
        let submission = self.disk.submit(Arc::new(req));
        let req = submission.complete().await;
        let res = req.response().unwrap();

        if let Err(e) = res {
            return Err(errno!(e.errno(), "write on a block device failed"));
        }
        Ok(())
    }

    fn clear_page_events(page_handle: &PageHandle<BlockId, A>) {
        page_handle.pollee().reset_events();
    }

    fn notify_page_events(page_handle: &PageHandle<BlockId, A>, events: Events) {
        page_handle.pollee().add_events(events);
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

struct CachedDiskFlusher<A: PageAlloc> {
    // this_opt => CachedDisk
    this_opt: Mutex<Option<Arc<Inner<A>>>>,
}

impl<A: PageAlloc> CachedDiskFlusher<A> {
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
impl<A: PageAlloc> PageCacheFlusher for CachedDiskFlusher<A> {
    async fn flush(&self) -> Result<usize> {
        if let Some(this) = self.this_opt() {
            return this.flush().await;
        }
        Ok(0)
    }
}
