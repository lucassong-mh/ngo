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

// Export SimpleFileSystem
pub use fs::AsyncSimpleFS;

mod cache;
mod fs;
mod prelude;
mod structs;
mod utils;

#[cfg(test)]
mod tests;
