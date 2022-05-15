use crate::prelude::*;

use std::alloc::Layout;
use std::marker::PhantomData;
use std::ptr::NonNull;

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
    pub fn new() -> Result<Self> {
        let ptr = A::alloc_page(Self::layout());
        let ptr = NonNull::new(ptr);
        if ptr.is_none() {
            return_errno!(ENOMEM, "page alloc failed, not enough memory");
        }
        Ok(Self {
            ptr: ptr.unwrap(),
            marker: PhantomData,
        })
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

impl<A: PageAlloc> Drop for Page<A> {
    fn drop(&mut self) {
        A::dealloc_page(self.ptr.as_ptr(), Self::layout());
    }
}
