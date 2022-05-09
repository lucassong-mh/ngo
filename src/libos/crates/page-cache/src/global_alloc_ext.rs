use lazy_static::lazy_static;

use std::alloc::{alloc, dealloc, GlobalAlloc, Layout};
use std::fmt::{Debug, Formatter};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// A global memory allocator that can monitor the amount
/// of free memory.
///
/// It is recommended to implement the trait with zero-sized
/// types.
///
/// ```
/// pub struct MyAlloc; // a zero-sized, singleton allocator
///
/// impl GlobalAllocExt for MyAlloc { /* ... */ }
/// impl GlobalAlloc for MyAlloc { /* ... */ }
/// ```
///
/// This way, embedding the allocator into other types
/// (e.g., `Page<MyAlloc>`) imposes no memory overhead.

pub trait GlobalAllocExt: GlobalAlloc + Send + Sync + Clone + Default + 'static {
    fn register_low_memory_callback(f: impl Fn());
    fn is_memory_low() -> bool;
}

/// A test-purpose fixed-size allocator
#[derive(Clone)]
pub struct FixedSizeAlloc {
    total_bytes: usize,
    remain_bytes: Arc<AtomicUsize>,
}

impl FixedSizeAlloc {
    pub fn new(total_bytes: usize) -> Self {
        Self {
            total_bytes,
            remain_bytes: Arc::new(AtomicUsize::new(total_bytes)),
        }
    }

    pub fn allocator_singleton() -> &'static FixedSizeAlloc {
        const ALLOC_CAPACITY: usize = 0x1000_0000;
        lazy_static! {
            pub(crate) static ref ALLOCATOR: FixedSizeAlloc = FixedSizeAlloc::new(ALLOC_CAPACITY);
        }
        &*ALLOCATOR
    }

    pub fn is_memory_low(&self) -> bool {
        const ALLOC_LIMIT: usize = 0x1_0000;
        self.remain_bytes.load(Ordering::Relaxed) < ALLOC_LIMIT
    }
}

impl Default for FixedSizeAlloc {
    fn default() -> Self {
        FixedSizeAlloc::allocator_singleton().clone()
    }
}

impl Debug for FixedSizeAlloc {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "FixedSizeAlloc {{ total_bytes: {}, remain_bytes: {} }}",
            self.total_bytes,
            self.remain_bytes.load(Ordering::Relaxed)
        )
    }
}

unsafe impl GlobalAlloc for FixedSizeAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.remain_bytes
            .fetch_sub(layout.size(), Ordering::Relaxed);
        alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.remain_bytes
            .fetch_add(layout.size(), Ordering::Relaxed);
        dealloc(ptr, layout)
    }
}

impl GlobalAllocExt for FixedSizeAlloc {
    fn register_low_memory_callback(f: impl Fn()) {
        f();
    }

    fn is_memory_low() -> bool {
        let allocator = FixedSizeAlloc::allocator_singleton();
        allocator.is_memory_low()
    }
}

/// A default zero-sized singleton allocator
#[derive(Clone, Default)]
pub struct MyAlloc;

unsafe impl GlobalAlloc for MyAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        dealloc(ptr, layout)
    }
}

impl GlobalAllocExt for MyAlloc {
    fn register_low_memory_callback(f: impl Fn()) {
        f();
    }
    fn is_memory_low() -> bool {
        false
    }
}
