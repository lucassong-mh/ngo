//! This crate provide the abstractions for page cache.
#![cfg_attr(feature = "sgx", no_std)]
#![feature(async_closure)]
#![feature(const_fn_trait_bound)]
#![feature(get_mut_unchecked)]
#![feature(in_band_lifetimes)]
#![feature(map_first_last)]

#[cfg(feature = "sgx")]
extern crate sgx_types;
#[cfg(feature = "sgx")]
#[macro_use]
extern crate sgx_tstd as std;
#[cfg(feature = "sgx")]
extern crate sgx_libc as libc;

#[macro_use]
extern crate log;

pub mod cached_disk;
mod page;
pub mod page_alloc;
pub mod page_cache;
mod page_evictor;
pub mod page_handle;
pub mod page_state;
mod prelude;
mod tests;
pub mod util;

pub use self::cached_disk::CachedDisk;
use self::page::Page;
pub use self::page_alloc::PageAlloc;
pub use self::page_cache::PageCache;
pub use self::page_cache::PageCacheFlusher;
use self::page_evictor::PageEvictor;
pub use self::page_handle::PageHandle;
pub use self::page_handle::PageKey;
pub use self::page_state::PageState;
pub use self::util::lru_cache::LruCache;
