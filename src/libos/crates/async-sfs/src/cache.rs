use crate::prelude::*;
use async_io::event::{Events, Poller};
use async_rt::wait::{Waiter, WaiterQueue};
use async_trait::async_trait;
use block_device::{BlockDevice, BlockDeviceExt};
use errno::prelude::Result;
use page_cache::{global_alloc_ext::MyAlloc, PageCache, PageCacheFlusher, PageHandle, PageState};

use spin::{Mutex, RwLock};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

pub(crate) struct SFSCache(Arc<Inner>);

struct Inner {
    device: Arc<dyn BlockDevice>,
    cache: PageCache<BlockId, MyAlloc>,
    flusher_wq: WaiterQueue,
    // This read-write lock is used to control the concurrent
    // writers and flushers. A writer acquires the read lock,
    // permitting multiple writers, but no flusher. A flusher
    // acquires the write lock to forbid any other flushers
    // and writers. This policy is important to implement
    // the semantic of the flush operation correctly.
    // TODO: AsyncRwLock,
    rw_lock: RwLock<usize>,
    // Whether fs is droppedd
    is_dropped: AtomicBool,
}

impl SFSCache {
    pub fn new(device: Arc<dyn BlockDevice>) -> Result<Self> {
        const CAPACITY: usize = 100_000;
        let flusher = Arc::new(SFSFlusher::new());
        let cache = PageCache::new(CAPACITY, flusher.clone());
        let flusher_wq = WaiterQueue::new();
        let arc_inner = Arc::new(Inner {
            device,
            cache,
            flusher_wq,
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
                // If fs is dropped, then the flusher task should exit
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
    pub async fn read(&self, from_bid: BlockId, buf: &mut [u8], offset: usize) -> Result<usize> {
        self.0.read(from_bid, buf, offset).await
    }

    /// Read blocks starting from `from_bid` into the given buffer.
    ///
    /// The length of buffer must be a multiple of BLOCK_SIZE. On
    /// success, the number of bytes written in returned.
    pub async fn write(&self, from_bid: BlockId, buf: &[u8], offset: usize) -> Result<usize> {
        self.0.write(from_bid, buf, offset).await
    }

    /// Flush all changes onto the backing disk.
    pub async fn flush(&self) -> Result<usize> {
        self.0.flush().await
    }
}

impl Drop for SFSCache {
    fn drop(&mut self) {
        self.0.is_dropped.store(true, Ordering::Relaxed);
        self.0.flusher_wq.wake_all();
    }
}

impl Inner {
    pub async fn read(&self, from_bid: BlockId, buf: &mut [u8], offset: usize) -> Result<usize> {
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
            let len = BLOCK_SIZE.min(buf.len());
            let dst_buf = &mut buf[i * BLOCK_SIZE..(i + 1) * len];
            let src_buf = page_guard.page().as_slice();
            dst_buf.copy_from_slice(&src_buf[offset..offset + len]);

            drop(page_guard);
            self.cache.release(&page_handle);
        }
        Ok(buf.len())
    }

    pub async fn write(&self, from_bid: BlockId, buf: &[u8], offset: usize) -> Result<usize> {
        let num_blocks = self.check_rw_args(from_bid, buf)?;
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

            let len = BLOCK_SIZE.min(buf.len());
            let dst_buf = page_guard.page().as_slice_mut();
            let src_buf = &buf[i * BLOCK_SIZE..(i + 1) * len];
            dst_buf[offset..offset + len].copy_from_slice(src_buf);
            page_guard.set_state(PageState::Dirty);

            drop(page_guard);
            self.cache.release(&page_handle);
        }
        drop(sem);
        Ok(buf.len())
    }

    pub async fn flush(&self) -> Result<usize> {
        let mut total_pages = 0;
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

        self.device.flush().await?;
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
        if from_bid >= self.device.total_blocks() {
            return Ok(0);
        }
        if buf.len() < BLOCK_SIZE {
            return Ok(1);
        }

        debug_assert!(buf.len() % BLOCK_SIZE == 0);

        let max_blocks = self.device.total_blocks() - from_bid;
        let num_blocks = buf.len() / BLOCK_SIZE;
        Ok(num_blocks.min(max_blocks))
    }

    async fn acquire_page(&self, block_id: BlockId) -> Result<PageHandle<BlockId, MyAlloc>> {
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
        self.device.read(offset, buf).await
    }

    async fn write_page(&self, block_id: &BlockId, buf: &[u8]) -> Result<usize> {
        let offset = block_id * BLOCK_SIZE;
        self.device.write(offset, buf).await
    }

    fn clear_page_events(page_handle: &PageHandle<BlockId, MyAlloc>) {
        page_handle.pollee().reset_events();
    }

    fn notify_page_events(page_handle: &PageHandle<BlockId, MyAlloc>, events: Events) {
        page_handle.pollee().add_events(events)
    }

    async fn wait_page_events(page_handle: &PageHandle<BlockId, MyAlloc>, events: Events) {
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
struct SFSFlusher {
    this_opt: Mutex<Option<Arc<Inner>>>,
}

impl SFSFlusher {
    pub fn new() -> Self {
        Self {
            this_opt: Mutex::new(None),
        }
    }

    pub fn set_disk(&self, this: Arc<Inner>) {
        *self.this_opt.lock() = Some(this);
    }

    fn this_opt(&self) -> Option<Arc<Inner>> {
        self.this_opt.lock().clone()
    }
}

#[async_trait]
impl PageCacheFlusher for SFSFlusher {
    async fn flush(&self) -> Result<usize> {
        if let Some(this) = self.this_opt() {
            this.flush().await?;
        }
        Ok(0)
    }
}
