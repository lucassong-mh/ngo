use crate::page_cache::PageCacheInner;
use crate::page_handle::PageKey;
use crate::prelude::*;
use crate::PageCache;
use block_device::AnyMap;
use lazy_static::lazy_static;
#[cfg(feature = "sgx")]
use sgx_trts::trts;

use std::future::Future;
use std::marker::PhantomData;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Page evictor.
///
/// Page caches (`PageCache<K, A>`) using the same memory allocator
/// (`A: GlobalAllocExt`) shares a common page evictor, which flushes
/// dirty pages and evict pages for the page caches when
/// the memory allocator's free memory is low.
// pub(crate) struct PageEvictor<A>;
pub(crate) struct PageEvictor<K: PageKey, A: GlobalAllocExt> {
    marker: PhantomData<(K, A)>,
}

impl<K: PageKey, A: GlobalAllocExt> PageEvictor<K, A> {
    /// Register a page cache.
    ///
    /// This is called in the constructor of a page
    /// cache instance.
    pub fn register(page_cache: &PageCache<K, A>) {
        let evictor_task = Self::task_singleton();
        evictor_task.register(&page_cache.0);
    }

    /// Unregister a page cache.
    pub fn unregister(page_cache: &PageCache<K, A>) {
        let evictor_task = Self::task_singleton();
        evictor_task.unregister(&page_cache.0);
    }

    fn task_singleton() -> Arc<EvictorTaskInner<K, A>> {
        lazy_static! {
            // Alternative: typemap::ShareCloneMap
            static ref EVICTOR_TASKS: Mutex<AnyMap> = Mutex::new(AnyMap::new());
        }

        let mut tasks = EVICTOR_TASKS.lock();
        tasks.insert(EvictorTask::<K, A>::new());
        tasks.get::<EvictorTask<K, A>>().unwrap().0.clone()
    }
}

#[derive(Clone)]
struct EvictorTask<K: PageKey, A: GlobalAllocExt>(Arc<EvictorTaskInner<K, A>>);

impl<K: PageKey, A: GlobalAllocExt> std::fmt::Debug for EvictorTask<K, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[PageEvictor EvictorTask]")
    }
}

struct EvictorTaskInner<K: PageKey, A: GlobalAllocExt> {
    caches: Mutex<Vec<Arc<PageCacheInner<K, A>>>>,
    wq: WaiterQueue,
    is_dropped: AtomicBool,
    marker: PhantomData<(K, A)>,
}

impl<K: PageKey, A: GlobalAllocExt> EvictorTask<K, A> {
    pub fn new() -> Self {
        let new_self = { Self(Arc::new(EvictorTaskInner::new())) };

        let this = new_self.0.clone();
        A::register_low_memory_callback(move || {
            this.wq.wake_all();
        });

        let this = new_self.0.clone();
        async_rt::task::spawn(async move {
            this.task_main().await;
        });

        new_self
    }
}

impl<K: PageKey, A: GlobalAllocExt> EvictorTaskInner<K, A> {
    pub fn new() -> Self {
        EvictorTaskInner {
            caches: Mutex::new(Vec::new()),
            wq: WaiterQueue::new(),
            is_dropped: AtomicBool::new(false),
            marker: PhantomData,
        }
    }

    pub fn register(&self, page_cache: &Arc<PageCacheInner<K, A>>) {
        let mut caches = self.caches.lock();
        caches.push(page_cache.clone());
    }

    pub fn unregister(&self, page_cache: &Arc<PageCacheInner<K, A>>) {
        let id = page_cache.id();
        let mut caches = self.caches.lock();
        caches.retain(|v| v.id() != id);
        self.is_dropped.store(true, Ordering::Relaxed);
    }

    async fn task_main(&self) {
        let mut waiter = Waiter::new();
        self.wq.enqueue(&mut waiter);
        while !self.is_dropped() {
            waiter.reset();

            while A::is_memory_low() {
                self.evict_pages().await;
            }

            waiter.wait().await;
        }
        self.wq.dequeue(&mut waiter);
    }

    async fn evict_pages(&self) {
        // Flush all page caches
        self.for_each_page_cache_async(async move |page_cache| {
            page_cache.flush().await;
        })
        .await;

        // Evict pages to free memory
        const BATCH_SIZE: usize = 2048;
        while A::is_memory_low() {
            let mut total_evicted = 0;
            self.for_each_page_cache(|page_cache| {
                total_evicted += page_cache.evict(BATCH_SIZE);
            });

            if total_evicted == 0 {
                break;
            }
        }
    }

    async fn for_each_page_cache_async<F, Fut>(&self, f: F)
    where
        F: Fn(Arc<PageCacheInner<K, A>>) -> Fut,
        Fut: Future<Output = ()>,
    {
        loop {
            if let Some(caches) = self.caches.try_lock() {
                if caches.len() > 0 {
                    cfg_if::cfg_if! {
                        // Load balance betwen the page caches
                        // so that pages are evenly evicted
                        if #[cfg(feature = "sgx")] {
                            let mut rand_arr = [0; 4];
                            trts::rsgx_read_rand(&mut rand_arr[..]);
                            let random: usize = rand_arr[0] as usize % caches.len();
                            for i in random..caches.len() {
                                f(caches[i].clone()).await;
                            }
                            for i in 0..random {
                                f(caches[i].clone()).await;
                            }
                        } else {
                            for i in 0..caches.len() {
                                f(caches[i].clone()).await;
                            }
                        }
                    }
                }
                drop(caches);
                break;
            }
            break;
        }
    }

    fn for_each_page_cache<F>(&self, mut f: F)
    where
        F: FnMut(&Arc<PageCacheInner<K, A>>),
    {
        let caches = self.caches.lock();
        if caches.len() > 0 {
            cfg_if::cfg_if! {
                // Random access
                if #[cfg(feature = "sgx")] {
                    let mut rand_arr = [0; 4];
                    trts::rsgx_read_rand(&mut rand_arr[..]);
                    let random: usize = rand_arr[0] as usize % caches.len();
                    for i in random..caches.len() {
                        f(&caches[i]);
                    }
                    for i in 0..random {
                        f(&caches[i]);
                    }
                } else {
                    for i in 0..caches.len() {
                        f(&caches[i]);
                    }
                }
            }
        }
        drop(caches);
    }

    fn is_dropped(&self) -> bool {
        self.is_dropped.load(Ordering::Relaxed)
    }
}
