use crate::prelude::*;

use std::alloc::Layout;
use std::ptr::NonNull;

/// A page obtained from an allocator.
pub struct Page<A: GlobalAllocExt> {
    // Alternative: AtomicPtr<u8>
    ptr: NonNull<u8>,
    allocator: A,
}

// Safety. GlobalAllocExt implements Send and Sync.
unsafe impl<A: GlobalAllocExt> Send for Page<A> {}
unsafe impl<A: GlobalAllocExt> Sync for Page<A> {}

impl<A: GlobalAllocExt> Page<A> {
    pub fn alloc_from(allocator: A) -> Option<Self> {
        let ptr = unsafe { allocator.alloc(Self::layout()) };
        let ptr = NonNull::new(ptr)?;
        Some(Self { ptr, allocator })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), Self::size()) }
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), Self::size()) }
    }

    pub const fn size() -> usize {
        4096
    }

    pub const fn align() -> usize {
        4096
    }

    pub const fn layout() -> Layout {
        unsafe { Layout::from_size_align_unchecked(Self::size(), Self::align()) }
    }
}

impl<A: GlobalAllocExt> Drop for Page<A> {
    fn drop(&mut self) {
        unsafe { self.allocator.dealloc(self.ptr.as_ptr(), Self::layout()) }
    }
}
