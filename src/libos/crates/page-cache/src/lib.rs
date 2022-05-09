//! This crate provide the abstractions for page cache.
#![cfg_attr(feature = "sgx", no_std)]
#![feature(get_mut_unchecked)]
#![feature(async_closure)]
#![feature(in_band_lifetimes)]
#![feature(const_fn_trait_bound)]

#[cfg(feature = "sgx")]
extern crate sgx_types;
#[cfg(feature = "sgx")]
#[macro_use]
extern crate sgx_tstd as std;
#[cfg(feature = "sgx")]
extern crate sgx_libc as libc;

#[macro_use]
extern crate log;

mod examples;
pub mod global_alloc_ext;
mod page;
pub mod page_cache;
mod page_evictor;
pub mod page_handle;
pub mod page_state;
mod prelude;
pub mod util;

pub use self::global_alloc_ext::GlobalAllocExt;
use self::page::Page;
pub use self::page_cache::PageCache;
pub use self::page_cache::PageCacheFlusher;
use self::page_evictor::PageEvictor;
pub use self::page_handle::PageHandle;
pub use self::page_state::PageState;
pub use self::util::lru_cache::LruCache;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global_alloc_ext::FixedSizeAlloc;
    use crate::prelude::*;
    use std::sync::Arc;

    struct Flusher;

    #[async_trait]
    impl PageCacheFlusher for Flusher {
        async fn flush(&self) -> Result<usize> {
            Ok(0)
        }
    }

    fn new_page_cache() -> PageCache<usize, FixedSizeAlloc> {
        let flusher = Arc::new(Flusher);
        const CAPACITY: usize = 10;
        PageCache::<usize, FixedSizeAlloc>::new(CAPACITY, flusher)
    }

    fn read_page(page_handle: &PageHandle<usize, FixedSizeAlloc>) -> u8 {
        let page_guard = page_handle.lock();
        page_guard.as_slice()[0]
    }

    fn write_page(page_handle: &PageHandle<usize, FixedSizeAlloc>, content: u8) {
        let mut page_guard = page_handle.lock();
        const SIZE: usize = Page::<FixedSizeAlloc>::size();
        page_guard.as_slice_mut().copy_from_slice(&[content; SIZE]);
    }

    #[test]
    fn acquire_release() {
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
            let dirty_num = cache.flush_dirty(&mut dirty);
            assert_eq!(dirty_num, 1);

            let page_handle = cache.acquire(key).unwrap();
            let mut page_guard = page_handle.lock();
            assert_eq!(page_guard.state(), PageState::Flushing);
            cache.0.flush().await;
            page_guard.set_state(PageState::UpToDate);
            drop(page_guard);
            cache.release(&page_handle);

            let mut dirty = Vec::with_capacity(128);
            let dirty_num = cache.flush_dirty(&mut dirty);
            assert_eq!(dirty_num, 0);
        })
    }

    #[test]
    fn page_cache_evict() {
        let cache = new_page_cache();
        const CAPACITY: usize = 10;
        for key in 0..CAPACITY + 5 {
            cache.acquire(key).unwrap();
        }

        assert_eq!(cache.size(), CAPACITY);
        const EVICT_NUM: usize = 5;
        let evict_num = cache.0.evict(EVICT_NUM);
        assert_eq!(evict_num, EVICT_NUM);
        assert_eq!(cache.size(), CAPACITY - EVICT_NUM);
    }
}
