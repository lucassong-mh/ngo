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
pub use fs::AsyncFileSystem;
pub use inode::AsyncInode;

mod dentry;
mod file_handle;
mod fs;
mod inode;
mod prelude;
