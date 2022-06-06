use crate::structs::AsBuf;

use block_device::{BlockDevice, BlockDeviceExt, BlockId, BLOCK_SIZE};
use errno::prelude::*;
use page_cache::{impl_page_alloc, CachedDisk};
use std::mem::MaybeUninit;
use std::sync::Arc;

// Define a page allocator `SFSPageAlloc: PageAlloc`
// with total bytes to let fs use page cache.
const MB: usize = 1024 * 1024;
impl_page_alloc! { SFSPageAlloc, 512 * MB }

pub enum SFSStorage {
    Device(Arc<dyn BlockDevice>),
    PageCache(CachedDisk<SFSPageAlloc>),
}

impl SFSStorage {
    pub fn from_device(device: Arc<dyn BlockDevice>) -> Self {
        Self::Device(device)
    }

    pub fn from_page_cache(cache: CachedDisk<SFSPageAlloc>) -> Self {
        Self::PageCache(cache)
    }

    /// Load struct `T` from given block and offset in the storage
    pub async fn load_struct<T: Sync + Send + AsBuf>(
        &self,
        id: BlockId,
        offset: usize,
    ) -> Result<T> {
        let mut s: T = unsafe { MaybeUninit::uninit().assume_init() };
        let s_mut_buf = s.as_buf_mut();
        debug_assert!(offset + s_mut_buf.len() <= BLOCK_SIZE);
        let device_offset = id * BLOCK_SIZE + offset;
        let len = match self {
            Self::Device(device) => device.read(device_offset, s_mut_buf).await?,
            Self::PageCache(cache) => cache.read(device_offset, s_mut_buf).await?,
        };
        debug_assert!(len == s_mut_buf.len());
        Ok(s)
    }

    /// Store struct `T` to given block and offset in the storage
    pub async fn store_struct<T: Sync + Send + AsBuf>(
        &self,
        id: BlockId,
        offset: usize,
        s: &T,
    ) -> Result<()> {
        let s_buf = s.as_buf();
        debug_assert!(offset + s_buf.len() <= BLOCK_SIZE);
        let device_offset = id * BLOCK_SIZE + offset;
        let len = match self {
            Self::Device(device) => device.write(device_offset, s_buf).await?,
            Self::PageCache(cache) => cache.write(device_offset, s_buf).await?,
        };
        debug_assert!(len == s_buf.len());
        Ok(())
    }

    /// Read blocks starting from the offset of block into the given buffer.
    pub async fn read_at(&self, id: BlockId, buf: &mut [u8], offset: usize) -> Result<usize> {
        let device_offset = id * BLOCK_SIZE + offset;
        match self {
            Self::Device(device) => device.read(device_offset, buf).await,
            Self::PageCache(cache) => cache.read(device_offset, buf).await,
        }
    }

    /// Write buffer at the blocks starting from the offset of block.
    pub async fn write_at(&self, id: BlockId, buf: &[u8], offset: usize) -> Result<usize> {
        let device_offset = id * BLOCK_SIZE + offset;
        match self {
            Self::Device(device) => device.write(device_offset, buf).await,
            Self::PageCache(cache) => cache.write(device_offset, buf).await,
        }
    }

    /// Flush the buffer.
    pub async fn flush(&self) -> Result<()> {
        match self {
            Self::Device(device) => device.flush().await,
            Self::PageCache(cache) => {
                cache.flush().await?;
                Ok(())
            }
        }
    }

    pub fn as_page_cache(&self) -> Option<&CachedDisk<SFSPageAlloc>> {
        match self {
            Self::PageCache(cache) => Some(cache),
            _ => None,
        }
    }
}
