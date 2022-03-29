use crate::prelude::*;

use crate::dentry::Dentry;
use async_io::event::{Events, Observer, Poller};
use async_io::file::{AccessMode, CreationFlags, StatusFlags};
use async_io::fs::{FileType, SeekFrom};
use async_io::ioctl::IoctlCmd;

/// The opened async inode through sys_open()
pub struct AsyncFileHandle {
    dentry: Dentry,
    offset: Mutex<usize>,
    access_mode: AccessMode,
    status_flags: RwLock<StatusFlags>,
}

impl AsyncFileHandle {
    pub async fn open(dentry: Dentry, flags: u32) -> Result<Self> {
        let access_mode = AccessMode::from_u32(flags)?;
        if access_mode.writable() && dentry.inode().metadata()?.type_ == FileType::Dir {
            return_errno!(EISDIR, "Directory cannot be open to write");
        }
        let creation_flags = CreationFlags::from_bits_truncate(flags);
        if creation_flags.should_truncate()
            && dentry.inode().metadata()?.type_ == FileType::File
            && access_mode.writable()
        {
            // truncate the length to 0
            dentry.inode().resize(0).await?;
        }
        let status_flags = StatusFlags::from_bits_truncate(flags);
        Ok(Self {
            dentry,
            offset: Mutex::new(0),
            access_mode,
            status_flags: RwLock::new(status_flags),
        })
    }

    pub async fn read(&self, buf: &mut [u8]) -> Result<usize> {
        if !self.access_mode.readable() {
            return_errno!(EBADF, "File not readable");
        }
        let mut offset = self.offset.lock();
        let len = self.dentry.inode().read_at(*offset, buf).await?;
        *offset += len;
        Ok(len)
    }

    pub async fn readv(&self, bufs: &mut [&mut [u8]]) -> Result<usize> {
        if !self.access_mode.readable() {
            return_errno!(EBADF, "File not readable");
        }
        let mut offset = self.offset.lock();
        let mut total_len = 0;
        for buf in bufs {
            match self.dentry.inode().read_at(*offset, buf).await {
                Ok(len) => {
                    total_len += len;
                    *offset += len;
                }
                Err(_) if total_len != 0 => break,
                Err(e) => return Err(e),
            }
        }
        Ok(total_len)
    }

    pub async fn write(&self, buf: &[u8]) -> Result<usize> {
        if !self.access_mode.writable() {
            return_errno!(EBADF, "File not writable");
        }
        let mut offset = self.offset.lock();
        if self.status_flags.read().always_append() {
            let info = self.dentry.inode().metadata()?;
            *offset = info.size;
        }
        let len = self.dentry.inode().write_at(*offset, buf).await?;
        *offset += len;
        Ok(len)
    }

    pub async fn writev(&self, bufs: &[&[u8]]) -> Result<usize> {
        if !self.access_mode.writable() {
            return_errno!(EBADF, "File not writable");
        }
        let mut offset = self.offset.lock();
        if self.status_flags.read().always_append() {
            let info = self.dentry.inode().metadata()?;
            *offset = info.size;
        }
        let mut total_len = 0;
        for buf in bufs {
            match self.dentry.inode().write_at(*offset, buf).await {
                Ok(len) => {
                    total_len += len;
                    *offset += len;
                }
                Err(_) if total_len != 0 => break,
                Err(e) => return Err(e),
            }
        }
        Ok(total_len)
    }

    pub fn seek(&self, pos: SeekFrom) -> Result<usize> {
        let mut offset = self.offset.lock();
        let new_offset: i64 = match pos {
            SeekFrom::Start(off /* as u64 */) => {
                if off > i64::max_value() as u64 {
                    return_errno!(EINVAL, "file offset is too large");
                }
                off as i64
            }
            SeekFrom::End(off /* as i64 */) => {
                let file_size = self.dentry.inode().metadata()?.size as i64;
                assert!(file_size >= 0);
                file_size
                    .checked_add(off)
                    .ok_or_else(|| errno!(EOVERFLOW, "file offset overflow"))?
            }
            SeekFrom::Current(off /* as i64 */) => (*offset as i64)
                .checked_add(off)
                .ok_or_else(|| errno!(EOVERFLOW, "file offset overflow"))?,
        };
        if new_offset < 0 {
            return_errno!(EINVAL, "file offset must not be negative");
        }
        // Invariant: 0 <= new_offset <= i64::max_value()
        let new_offset = new_offset as usize;
        *offset = new_offset;
        Ok(new_offset)
    }

    pub fn poll(&self, mask: Events, _poller: Option<&mut Poller>) -> Events {
        let events = match self.access_mode {
            AccessMode::O_RDONLY => Events::IN,
            AccessMode::O_WRONLY => Events::OUT,
            AccessMode::O_RDWR => Events::IN | Events::OUT,
        };
        events | mask
    }

    pub fn register_observer(&self, _observer: Arc<dyn Observer>, _mask: Events) -> Result<()> {
        return_errno!(EINVAL, "do not support observers");
    }

    pub fn unregister_observer(&self, _observer: &Arc<dyn Observer>) -> Result<Arc<dyn Observer>> {
        return_errno!(EINVAL, "do not support observers");
    }

    pub fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    pub fn status_flags(&self) -> StatusFlags {
        let status_flags = self.status_flags.read();
        *status_flags
    }

    pub fn set_status_flags(&self, new_status_flags: StatusFlags) -> Result<()> {
        let mut status_flags = self.status_flags.write();
        // Currently, F_SETFL can change only the O_APPEND,
        // O_ASYNC, O_NOATIME, and O_NONBLOCK flags
        let valid_flags_mask = StatusFlags::O_APPEND
            | StatusFlags::O_ASYNC
            | StatusFlags::O_NOATIME
            | StatusFlags::O_NONBLOCK;
        status_flags.remove(valid_flags_mask);
        status_flags.insert(new_status_flags & valid_flags_mask);
        Ok(())
    }

    pub fn ioctl(&self, cmd: &mut dyn IoctlCmd) -> Result<()> {
        self.dentry.inode().ioctl(cmd)
    }

    pub fn dentry(&self) -> &Dentry {
        &self.dentry
    }
}

impl std::fmt::Debug for AsyncFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "AsyncFileHandle {{ dentry: ???, offset: {}, access_mode: {:?}, status_flags: {:#o} }}",
            *self.offset.lock(),
            self.access_mode,
            *self.status_flags.read()
        )
    }
}
