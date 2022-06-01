use crate::prelude::*;
use crate::LruCache;
use crate::PageEvictor;
use object_id::ObjectId;

use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;

/// Page cache.
pub struct PageCache<K: PageKey, A: PageAlloc>(pub(crate) Arc<PageCacheInner<K, A>>);

type PageId = usize;

pub(crate) struct PageCacheInner<K: PageKey, A: PageAlloc> {
    id: ObjectId,
    flusher: Arc<dyn PageCacheFlusher>,
    cache: Mutex<LruCache<PageId, PageHandle<K, A>>>,
    dirty_set: Mutex<BTreeSet<PageId>>,
    pollee: Pollee,
    marker: PhantomData<(K, A)>,
}

/// Page cache flusher.
///
/// A page cache must be equiped with a user-given
/// flusher `F: PageCacheFlusher`so that when
/// the memory is low, the page cache mechanism
/// can automatically flush dirty pages and
/// subsequently evict pages.
///
/// This trait has only one method.
#[async_trait]
pub trait PageCacheFlusher: Send + Sync {
    /// Flush the dirty pages in a page cache.
    ///
    /// If success, then the return value is
    /// the number of dirty pages that are flushed.
    async fn flush(&self) -> Result<usize>;
}

impl<K: PageKey, A: PageAlloc> PageCache<K, A> {
    /// Create a page cache.
    pub fn new(flusher: Arc<dyn PageCacheFlusher>) -> Self {
        info!("[PageCache] new");
        let new_self = Self(Arc::new(PageCacheInner::new(flusher)));
        PageEvictor::<K, A>::register(&new_self);
        new_self
    }

    /// Acquire the page that corresponds to the key.
    ///
    /// Returns `None` if there are no available pages.
    /// In this case, the user can use the
    /// `poll` method to wait for the readiness of the
    /// page cache.
    pub fn acquire(&self, key: K) -> Option<PageHandle<K, A>> {
        let mut cache = self.0.cache.lock();
        // Cache hit
        if let Some(page_handle_incache) = cache.get(&key.into()) {
            return Some(page_handle_incache.clone());
        // Cache miss
        } else {
            let page_handle = PageHandle::new(key);
            cache.put(key.into(), page_handle.clone());
            return Some(page_handle);
        }
    }

    /// Release the page.
    ///
    /// All page handles obtained via the `acquire` method
    /// must be returned via the `release` method.
    pub fn release(&self, page_handle: &PageHandle<K, A>) {
        let mut dirty_set = self.0.dirty_set.lock();
        let page_guard = page_handle.lock();
        // Update dirty_set when release
        if page_guard.state() == PageState::Dirty {
            dirty_set.insert(page_handle.key().into());
        } else {
            dirty_set.remove(&page_handle.key().into());
        }
    }

    /// Pop a number of dirty pages and switch their state to
    /// "Flushing".
    ///
    /// The handles of dirty pages are pushed into the given `Vec`.
    /// The dirty pages are always consecutive per call.
    /// Return flush numbers and the first page/block id.
    pub fn flush_dirty(&self, dirty: &mut Vec<PageHandle<K, A>>) -> (usize, PageId) {
        let cache = self.0.cache.lock();
        // The dirty_set traces dirty pages
        let mut dirty_set = self.0.dirty_set.lock();
        let mut flush_num = 0;
        let mut first_page_key: PageId = 0;
        let mut pre_key: PageId = 0;

        // Handle first valid page
        loop {
            match dirty_set.pop_first() {
                Some(first_key) => {
                    if let Some(page_handle_incache) = cache.just_get(&first_key) {
                        let mut page_guard = page_handle_incache.lock();
                        debug_assert!(page_guard.state() == PageState::Dirty);

                        page_guard.set_state(PageState::Flushing);
                        dirty.push(page_handle_incache.clone());
                        flush_num += 1;
                        drop(page_guard);

                        first_page_key = first_key;
                        pre_key = first_key;
                        break;
                    }
                }
                None => {
                    return (0, pre_key);
                }
            }
        }

        // Handle following valid pages
        while let Some(cur_key) = dirty_set.pop_first() {
            // Ensure always return consecutive pages
            if cur_key - pre_key > 1 {
                break;
            }

            if let Some(page_handle_incache) = cache.just_get(&cur_key) {
                let mut page_guard = page_handle_incache.lock();
                debug_assert!(page_guard.state() == PageState::Dirty);

                page_guard.set_state(PageState::Flushing);
                dirty.push(page_handle_incache.clone());
                flush_num += 1;
                drop(page_guard);

                pre_key = cur_key;
            }
        }

        (flush_num, first_page_key)
    }

    pub fn size(&self) -> usize {
        let cache = self.0.cache.lock();
        cache.size()
    }

    /// Poll the readiness events on a page cache.
    ///
    /// The only interesting event is `Events::OUT`, which
    /// indicates that the page cache has evictable pages or
    /// the underlying page allocator has free space.
    ///
    /// This method is typically used after a failed attempt to
    /// acquire pages. In such situations, one needs to wait
    /// for the page cache to be ready for acquiring new pages.
    ///
    /// ```
    /// # async fn foo<A: PageAlloc>(page_cache: &PageCache<u64, A>) {
    /// let addr = 1234;
    /// let page = loop {
    ///     if Some(page) = page_cache.acquire(addr) {
    ///         break page;
    ///     }
    ///     
    ///     let mut poller = Poller::new();
    ///     let events = page_cache.poll(Events::OUT, Some(&mut poller));
    ///     if !events.is_empty() {
    ///         continue;
    ///     }
    ///
    ///     poller.wait().await;
    /// }
    /// # }
    /// ```
    pub fn poll(&self, poller: Option<&mut Poller>) -> Events {
        self.0.poll(poller)
    }
}

impl<K: PageKey, A: PageAlloc> PageCacheInner<K, A> {
    pub fn new(flusher: Arc<dyn PageCacheFlusher>) -> Self {
        PageCacheInner {
            id: ObjectId::new(),
            flusher,
            cache: Mutex::new(LruCache::new(0)),
            dirty_set: Mutex::new(BTreeSet::new()),
            pollee: Pollee::new(Events::empty()),
            marker: PhantomData,
        }
    }

    pub const fn id(&self) -> ObjectId {
        self.id
    }

    pub fn poll(&self, poller: Option<&mut Poller>) -> Events {
        self.pollee.poll(Events::OUT, poller)
    }

    /// Evict a number of pages.
    ///
    /// The page cache uses a pseudo-LRU strategy to select
    /// the victim pages.
    pub(crate) fn evict(&self, max_evicted: usize) -> usize {
        let mut cache = self.cache.lock();
        let evict_total = max_evicted.min(cache.size());
        let mut evict_num = 0;
        for _ in 0..evict_total {
            if let Some(page_handle) = cache.evict() {
                let mut page_guard = page_handle.lock();
                drop(page_guard.page());
                evict_num += 1;
                drop(page_guard);
                drop(page_handle);
            }
        }
        evict_num
    }

    pub(crate) async fn flush(&self) {
        let nflush = self.flusher.flush().await.unwrap();
        if nflush > 0 {
            trace!("[PageCache] flush pages: {}", nflush);
            self.pollee.add_events(Events::OUT);
        }
    }
}

impl<K: PageKey, A: PageAlloc> Drop for PageCache<K, A> {
    fn drop(&mut self) {
        PageEvictor::<K, A>::unregister(&self);
    }
}
