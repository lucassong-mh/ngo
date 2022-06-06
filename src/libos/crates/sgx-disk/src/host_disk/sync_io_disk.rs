use block_device::{BioReq, BioSubmission, BioType, BlockDevice};
use fs::File;
use std::io::prelude::*;
use std::io::{IoSlice, IoSliceMut, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use super::OpenOptions;
use crate::prelude::*;
use crate::HostDisk;

/// A type of host disk that implements a block device interface by performing
/// normal synchronous I/O to the underlying host file.
///
/// `SyncIoDisk` implements the interface of `BlockDevice`. Although the
/// interface is asynchronous, the implementation uses the normal synchronous
/// `read` and `write` system calls to perform I/O. Thus, the performance of
/// `SyncIoDisk` is not good. This is especially true for SGX where issuing
/// system calls from the enclave triggers enclave switching, which is costly.
///
/// It is recommended to use `IoUringDisk` for an optimal performance.
#[derive(Debug)]
pub struct SyncIoDisk {
    file: File,
    path: PathBuf,
    total_blocks: usize,
    can_read: bool,
    can_write: bool,
}

impl SyncIoDisk {
    fn do_read(&self, req: &Arc<BioReq>) -> Result<()> {
        if !self.can_read {
            return Err(errno!(EACCES, "read is not allowed"));
        }

        let (mut offset, _) = self.get_range_in_bytes(&req)?;

        let read_len = req.access_mut_bufs_with(|bufs| {
            // TODO: fix this limitation
            const LINUX_IOVS_MAX: usize = 1024;
            debug_assert!(bufs.len() < LINUX_IOVS_MAX);

            let mut read_len = 0usize;
            for buf in bufs {
                if let Ok(len) = self.file.read_at(buf.as_slice_mut(), offset as u64) {
                    read_len += len;
                }
                offset += BLOCK_SIZE;
            }
            read_len
        });

        debug_assert!(read_len / BLOCK_SIZE == req.num_blocks());
        Ok(())
    }

    fn do_write(&self, req: &Arc<BioReq>) -> Result<()> {
        if !self.can_write {
            return Err(errno!(EACCES, "write is not allowed"));
        }

        let (mut offset, _) = self.get_range_in_bytes(&req)?;

        let write_len = req.access_bufs_with(|bufs| {
            // TODO: fix this limitation
            const LINUX_IOVS_MAX: usize = 1024;
            debug_assert!(bufs.len() < LINUX_IOVS_MAX);

            let mut write_len = 0usize;
            for buf in bufs {
                if let Ok(len) = self.file.write_at(buf.as_slice(), offset as u64) {
                    write_len += len;
                }
                offset += BLOCK_SIZE;
            }
            write_len
        });

        debug_assert!(write_len / BLOCK_SIZE == req.num_blocks());
        Ok(())
    }

    fn do_flush(&self) -> Result<()> {
        if !self.can_write {
            return Err(errno!(EACCES, "flush is not allowed"));
        }

        self.file.sync_data()?;

        Ok(())
    }

    fn get_range_in_bytes(&self, req: &Arc<BioReq>) -> Result<(usize, usize)> {
        let begin_block = req.addr();
        let end_block = begin_block + req.num_blocks();
        if end_block > self.total_blocks {
            return Err(errno!(EINVAL, "invalid block range"));
        }
        let begin_offset = begin_block * BLOCK_SIZE;
        let end_offset = end_block * BLOCK_SIZE;
        Ok((begin_offset, end_offset))
    }
}

impl BlockDevice for SyncIoDisk {
    fn total_blocks(&self) -> usize {
        self.total_blocks
    }

    fn submit(&self, req: Arc<BioReq>) -> BioSubmission {
        // Update the status of req to submittted
        let submission = BioSubmission::new(req);

        let req = submission.req();
        let type_ = req.type_();
        let res = match type_ {
            BioType::Read => self.do_read(req),
            BioType::Write => self.do_write(req),
            BioType::Flush => self.do_flush(),
        };

        // Update the status of req to completed and set the response
        let resp = res.map_err(|e| e.errno());
        unsafe {
            req.complete(resp);
        }

        submission
    }
}

impl HostDisk for SyncIoDisk {
    fn from_options_and_file(options: &OpenOptions<Self>, file: File, path: &Path) -> Result<Self> {
        let total_blocks = options.total_blocks.unwrap_or_else(|| {
            let file_len = file.metadata().unwrap().len() as usize;
            assert!(file_len >= BLOCK_SIZE);
            file_len / BLOCK_SIZE
        });
        let can_read = options.read;
        let can_write = options.write;
        let path = path.to_owned();
        let new_self = Self {
            file,
            path,
            total_blocks,
            can_read,
            can_write,
        };
        Ok(new_self)
    }

    fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl Drop for SyncIoDisk {
    fn drop(&mut self) {
        // Ensure all data are peristed before the disk is dropped
        let _ = self.do_flush();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_setup() -> SyncIoDisk {
        // As unit tests may run concurrently, they must operate on different
        // files. This helper function generates unique file paths.
        fn gen_unique_path() -> String {
            use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

            static UT_ID: AtomicU32 = AtomicU32::new(0);

            let ut_id = UT_ID.fetch_add(1, Relaxed);
            format!("sync_io_disk{}.image", ut_id)
        }

        let total_blocks = 16;
        let path = gen_unique_path();
        SyncIoDisk::create(&path, total_blocks).unwrap()
    }

    fn test_teardown(disk: SyncIoDisk) {
        let _ = std::fs::remove_file(disk.path());
    }

    block_device::gen_unit_tests!(test_setup, test_teardown);
}
