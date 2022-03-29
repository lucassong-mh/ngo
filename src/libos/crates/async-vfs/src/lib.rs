#![cfg_attr(feature = "sgx", no_std)]

#[cfg(feature = "sgx")]
extern crate sgx_types;
#[cfg(feature = "sgx")]
#[macro_use]
extern crate sgx_tstd as std;
#[cfg(feature = "sgx")]
extern crate sgx_libc as libc;

#[macro_use]
extern crate log;

pub use dentry::Dentry;
pub use file_handle::AsyncFileHandle;
pub use inode::{AsyncFileSystem, AsyncInode};

mod dentry;
mod file_handle;
mod inode;
mod prelude;
