use crate::prelude::*;
use block_device::BLOCK_SIZE;

use std::alloc::Layout;
use std::marker::PhantomData;
use std::ptr::NonNull;

const PAGE_SIZE: usize = BLOCK_SIZE;

/// A page obtained from an allocator.
pub struct Page<A: PageAlloc> {
    // Alternative: AtomicPtr<u8>
    ptr: NonNull<u8>,
    marker: PhantomData<A>,
}

// Safety. PageAlloc implements Send and Sync.
unsafe impl<A: PageAlloc> Send for Page<A> {}
unsafe impl<A: PageAlloc> Sync for Page<A> {}

impl<A: PageAlloc> Page<A> {
    pub fn new() -> Option<Self> {
        let page_ptr = A::alloc_page(Self::layout());
        if let Some(ptr) = NonNull::new(page_ptr) {
            return Some(Self {
                ptr,
                marker: PhantomData,
            });
        }
        return None;
    }

    #[inline]
    pub fn as_ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), Self::size()) }
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), Self::size()) }
    }

    #[inline]
    pub const fn size() -> usize {
        PAGE_SIZE
    }

    #[inline]
    pub const fn align() -> usize {
        PAGE_SIZE
    }

    #[inline]
    pub const fn layout() -> Layout {
        unsafe { Layout::from_size_align_unchecked(Self::size(), Self::align()) }
    }
}

impl<A: PageAlloc> Drop for Page<A> {
    fn drop(&mut self) {
        A::dealloc_page(self.ptr.as_ptr(), Self::layout());
    }
}
