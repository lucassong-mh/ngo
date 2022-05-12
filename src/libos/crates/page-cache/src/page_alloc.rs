use std::alloc::{alloc, dealloc, Layout};
use std::fmt::{Debug, Formatter};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// A page allocator that can monitor the amount of free memory.
/// It is recommended to implement the trait with zero-sized types.
pub trait PageAlloc: Send + Sync + Clone + 'static {
    fn alloc_page(layout: Layout) -> *mut u8;

    fn dealloc_page(ptr: *mut u8, layout: Layout);

    fn register_low_memory_callback(f: impl Fn());

    fn is_memory_low() -> bool;
}

/// A fixed-size page allocator
#[derive(Clone)]
pub struct FixedSizePageAlloc {
    total_bytes: usize,
    remain_bytes: Arc<AtomicUsize>,
}

impl FixedSizePageAlloc {
    pub fn new(total_bytes: usize) -> Self {
        let new_self = Self {
            total_bytes,
            remain_bytes: Arc::new(AtomicUsize::new(total_bytes)),
        };
        trace!("[FixedSizePageAlloc] {:#?}", new_self);
        new_self
    }

    pub fn alloc_page(&self, layout: Layout) -> *mut u8 {
        let cost_bytes = self.remain_bytes.load(Ordering::Relaxed).min(layout.size());
        self.remain_bytes.fetch_sub(cost_bytes, Ordering::Relaxed);
        unsafe { alloc(layout) }
    }

    pub fn dealloc_page(&self, ptr: *mut u8, layout: Layout) {
        self.remain_bytes
            .fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { dealloc(ptr, layout) }
    }

    pub fn is_memory_low(&self) -> bool {
        const ALLOC_LIMIT: usize = 0x1_0000;
        self.remain_bytes.load(Ordering::Relaxed) < ALLOC_LIMIT
    }
}

impl Debug for FixedSizePageAlloc {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "FixedSizePageAlloc {{ total_bytes: {}, remain_bytes: {} }}",
            self.total_bytes,
            self.remain_bytes.load(Ordering::Relaxed)
        )
    }
}

/// A macro to define a fixed-size allocator with total bytes
/// which implement the `PageAlloc` trait.
///
/// ```
/// impl_page_alloc! { MyPageAlloc, 1024 }
/// ```
#[macro_export]
macro_rules! impl_page_alloc {
    ($page_alloc:ident, $total_bytes:expr) => {
        use lazy_static::lazy_static;
        use std::alloc::Layout;
        use $crate::page_alloc::{FixedSizePageAlloc, PageAlloc};

        #[derive(Clone)]
        pub struct $page_alloc;

        lazy_static! {
            static ref ALLOCATOR: FixedSizePageAlloc = FixedSizePageAlloc::new($total_bytes);
        }

        impl PageAlloc for $page_alloc {
            fn alloc_page(layout: Layout) -> *mut u8 {
                ALLOCATOR.alloc_page(layout)
            }

            fn dealloc_page(ptr: *mut u8, layout: Layout) {
                ALLOCATOR.dealloc_page(ptr, layout);
            }

            fn register_low_memory_callback(f: impl Fn()) {
                f();
            }

            fn is_memory_low() -> bool {
                ALLOCATOR.is_memory_low()
            }
        }
    };
}
