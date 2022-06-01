use crate::impl_page_alloc;
use crate::prelude::*;
use crate::Page;
use block_device::{mem_disk::MemDisk, BLOCK_SIZE};

use std::sync::Arc;
use std::time::Duration;

// MyPageAlloc is a test-purpose allocator
impl_page_alloc! { MyPageAlloc, 1024 * 1024 * 512 }

/// PageCache tests
struct SimpleFlusher;

#[async_trait]
impl PageCacheFlusher for SimpleFlusher {
    async fn flush(&self) -> Result<usize> {
        Ok(0)
    }
}

fn new_page_cache() -> PageCache<usize, MyPageAlloc> {
    let flusher = Arc::new(SimpleFlusher);
    PageCache::<usize, MyPageAlloc>::new(flusher)
}

fn read_page(page_handle: &PageHandle<usize, MyPageAlloc>) -> u8 {
    let page_guard = page_handle.lock();
    page_guard.as_slice()[0]
}

fn write_page(page_handle: &PageHandle<usize, MyPageAlloc>, content: u8) {
    let mut page_guard = page_handle.lock();
    const SIZE: usize = Page::<MyPageAlloc>::size();
    page_guard.as_slice_mut().copy_from_slice(&[content; SIZE]);
}

#[test]
fn page_cache_acquire_release() {
    let cache = new_page_cache();
    let key: usize = 125;
    let content: u8 = 5;

    let page_handle = cache.acquire(key).unwrap();
    let mut page_guard = page_handle.lock();
    assert_eq!(page_guard.state(), PageState::Uninit);
    page_guard.set_state(PageState::Dirty);
    drop(page_guard);

    write_page(&page_handle, content);
    cache.release(&page_handle);
    assert_eq!(cache.size(), 1);

    let page_handle = cache.acquire(key).unwrap();
    assert_eq!(page_handle.key(), key);
    let page_guard = page_handle.lock();
    assert_eq!(page_guard.state(), PageState::Dirty);
    drop(page_guard);

    let read_content = read_page(&page_handle);
    assert_eq!(read_content, content);
    cache.release(&page_handle);
    assert_eq!(cache.size(), 1);
}

#[test]
fn page_cache_flush() {
    async_rt::task::block_on(async move {
        let cache = new_page_cache();
        let key: usize = 125;

        let page_handle = cache.acquire(key).unwrap();
        let mut page_guard = page_handle.lock();
        page_guard.set_state(PageState::Dirty);
        drop(page_guard);
        cache.release(&page_handle);

        let mut dirty = Vec::with_capacity(128);
        let (dirty_num, _) = cache.flush_dirty(&mut dirty);
        assert_eq!(dirty_num, 1);

        let page_handle = cache.acquire(key).unwrap();
        let mut page_guard = page_handle.lock();
        assert_eq!(page_guard.state(), PageState::Flushing);
        cache.0.flush().await;
        page_guard.set_state(PageState::UpToDate);
        drop(page_guard);
        cache.release(&page_handle);

        let mut dirty = Vec::with_capacity(128);
        let (dirty_num, _) = cache.flush_dirty(&mut dirty);
        assert_eq!(dirty_num, 0);
    })
}

#[test]
fn page_cache_evict() {
    let cache = new_page_cache();
    const CAPACITY: usize = 15;
    for key in 0..CAPACITY {
        cache.acquire(key).unwrap();
    }

    assert_eq!(cache.size(), CAPACITY);
    const EVICT_NUM: usize = 5;
    let evict_num = cache.0.evict(EVICT_NUM);
    assert_eq!(evict_num, EVICT_NUM);
    assert_eq!(cache.size(), CAPACITY - EVICT_NUM);

    let evict_num = cache.0.evict(CAPACITY);
    assert_eq!(evict_num, CAPACITY - EVICT_NUM);
    assert_eq!(cache.size(), 0);
}

#[test]
fn page_cache_evictor_task() {
    let cache = new_page_cache();
    const CAPACITY: usize = 1_0000;
    for key in 0..CAPACITY {
        cache.acquire(key).unwrap();
    }

    // Pages being evicted during out-limit acquire
    assert!(cache.size() <= CAPACITY);
}

/// CachedDisk tests
fn new_cached_disk() -> CachedDisk<MyPageAlloc> {
    const TOTAL_BLOCKS: usize = 1024 * 1024;
    let mem_disk = MemDisk::new(TOTAL_BLOCKS).unwrap();
    CachedDisk::<MyPageAlloc>::new(Arc::new(mem_disk)).unwrap()
}

#[test]
fn cached_disk_read_write() -> Result<()> {
    async_rt::task::block_on(async move {
        let cached_disk = new_cached_disk();
        let content: u8 = 5;
        const SIZE: usize = BLOCK_SIZE * 1;
        const OFFSET: usize = 1024;

        let mut read_buf: [u8; SIZE] = [0; SIZE];
        let len = cached_disk.read(OFFSET, &mut read_buf[..]).await?;
        assert_eq!(SIZE, len, "[CachedDisk] read failed");

        let write_buf: [u8; SIZE] = [content; SIZE];
        let len = cached_disk.write(OFFSET, &write_buf[..]).await?;
        assert_eq!(SIZE, len, "[CachedDisk] write failed");

        let len = cached_disk.read(OFFSET, &mut read_buf[..]).await?;
        assert_eq!(SIZE, len, "[CachedDisk] read failed");
        assert_eq!(read_buf, write_buf, "[CachedDisk] read wrong content");

        let rw_cnt = 10_0000;
        let mut buf: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
        for i in 0..rw_cnt {
            let offset = i * BLOCK_SIZE;
            cached_disk.read(offset, &mut buf[..]).await?;
            cached_disk.write(offset, &buf[..]).await?;
        }
        Ok(())
    })
}

#[test]
fn cached_disk_flush() -> Result<()> {
    async_rt::task::block_on(async move {
        let cached_disk = new_cached_disk();
        const SIZE: usize = BLOCK_SIZE;
        let write_cnt = 1;
        for i in 0..write_cnt {
            let offset = i * BLOCK_SIZE;
            let write_buf: [u8; SIZE] = [0; SIZE];
            let len = cached_disk.write(offset, &write_buf[..]).await?;
            assert_eq!(SIZE, len, "[CachedDisk] write failed");
        }
        let flush_num = cached_disk.flush().await?;
        assert_eq!(flush_num, write_cnt, "[CachedDisk] flush failed");

        let write_cnt = 1000;
        for i in 0..write_cnt {
            let offset = i * BLOCK_SIZE;
            let write_buf: [u8; SIZE] = [0; SIZE];
            let len = cached_disk.write(offset, &write_buf[..]).await?;
            assert_eq!(SIZE, len, "[CachedDisk] write failed");
        }
        let flush_num = cached_disk.flush().await?;
        assert_eq!(flush_num, write_cnt, "[CachedDisk] flush failed");
        Ok(())
    })
}

#[test]
fn cached_disk_flusher_task() -> Result<()> {
    async_rt::task::block_on(async move {
        let cached_disk = Arc::new(new_cached_disk());
        let reader = cached_disk.clone();
        let writer = cached_disk.clone();
        const SIZE: usize = 4096;
        let rw_cnt = 10;

        let writer_handle = async_rt::task::spawn(async move {
            for _ in 0..rw_cnt {
                let write_buf: [u8; SIZE] = [0; SIZE];
                writer.write(0, &write_buf[..]).await.unwrap();
            }
        });
        let reader_handle = async_rt::task::spawn(async move {
            let waiter = Waiter::new();
            for _ in 0..rw_cnt {
                waiter
                    .wait_timeout(Some(&mut Duration::from_millis(500)))
                    .await;
                let mut read_buf: [u8; SIZE] = [0; SIZE];
                reader.read(0, &mut read_buf[..]).await.unwrap();
            }
        });

        writer_handle.await;
        reader_handle.await;

        let flush_num = cached_disk.flush().await?;
        // Pages are already flushed by flusher task
        assert_eq!(flush_num, 0, "[CachedDisk] flush failed");
        Ok(())
    })
}
