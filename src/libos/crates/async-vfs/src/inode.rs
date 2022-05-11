use crate::prelude::*;

use async_io::fs::{FileType, FsInfo, Metadata, PATH_MAX};
use async_io::ioctl::IoctlCmd;
use async_trait::async_trait;
use std::any::Any;

/// Abstract Async Inode object such as file or directory.
#[async_trait]
pub trait AsyncInode: Any + Sync + Send {
    /// Read bytes at `offset` into `buf`, return the number of bytes read.
    async fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize>;

    /// Write bytes at `offset` from `buf`, return the number of bytes written.
    async fn write_at(&self, offset: usize, buf: &[u8]) -> Result<usize>;

    /// Get metadata of the INode
    async fn metadata(&self) -> Result<Metadata> {
        return_errno!(ENOSYS, "not support");
    }

    /// Set metadata of the INode
    async fn set_metadata(&self, _metadata: &Metadata) -> Result<()> {
        return_errno!(ENOSYS, "not support");
    }

    /// Sync all data and metadata
    async fn sync_all(&self) -> Result<()> {
        return_errno!(ENOSYS, "not support");
    }

    /// Sync data (not include metadata)
    async fn sync_data(&self) -> Result<()> {
        return_errno!(ENOSYS, "not support");
    }

    /// Resize the file
    async fn resize(&self, _len: usize) -> Result<()> {
        return_errno!(ENOSYS, "not support");
    }

    /// Create a new INode in the directory
    async fn create(
        &self,
        _name: &str,
        _type_: FileType,
        _mode: u16,
    ) -> Result<Arc<dyn AsyncInode>> {
        return_errno!(ENOSYS, "not support");
    }

    /// Create a hard link `name` to `other`
    async fn link(&self, _name: &str, _other: &Arc<dyn AsyncInode>) -> Result<()> {
        return_errno!(ENOSYS, "not support");
    }

    /// Delete a hard link `name`
    async fn unlink(&self, _name: &str) -> Result<()> {
        return_errno!(ENOSYS, "not support");
    }

    /// Move INode `self/old_name` to `target/new_name`.
    /// If `target` equals `self`, do rename.
    async fn move_(
        &self,
        _old_name: &str,
        _target: &Arc<dyn AsyncInode>,
        _new_name: &str,
    ) -> Result<()> {
        return_errno!(ENOSYS, "not support");
    }

    /// Find the INode `name` in the directory
    async fn find(&self, _name: &str) -> Result<Arc<dyn AsyncInode>> {
        return_errno!(ENOSYS, "not support");
    }

    /// Read the content of symlink
    async fn read_link(&self) -> Result<String>;

    /// Write the content of symlink
    async fn write_link(&self, target: &str) -> Result<()>;

    /// Get the name of directory entry
    async fn get_entry(&self, _id: usize) -> Result<String> {
        return_errno!(ENOSYS, "not support");
    }

    /// Control device
    fn ioctl(&self, _cmd: &mut dyn IoctlCmd) -> Result<()> {
        return_errno!(ENOSYS, "not support");
    }

    /// Get the file system of the INode
    fn fs(&self) -> Arc<dyn AsyncFileSystem> {
        unimplemented!();
    }

    /// This is used to implement dynamics cast.
    /// Simply return self in the implement of the function.
    fn as_any_ref(&self) -> &dyn Any;

    /// Lookup path from current INode, and do not follow symlinks
    async fn lookup(&self, path: &str) -> Result<Arc<dyn AsyncInode>> {
        let inode = self.lookup_follow(path, 0).await?;
        Ok(inode)
    }

    /// Lookup path from current INode, and follow symlinks at most `max_follow_times` times
    async fn lookup_follow(
        &self,
        path: &str,
        max_follow_times: usize,
    ) -> Result<Arc<dyn AsyncInode>> {
        if self.metadata().await?.type_ != FileType::Dir {
            return_errno!(ENOTDIR, "not dir");
        }
        if path.len() > PATH_MAX {
            return_errno!(ENAMETOOLONG, "name too long");
        }

        let mut follow_times = 0;
        let mut result = self.find(".").await?;
        let mut rest_path = String::from(path);
        while rest_path != "" {
            if result.metadata().await?.type_ != FileType::Dir {
                return_errno!(ENOTDIR, "not dir");
            }
            // handle absolute path
            if let Some('/') = rest_path.chars().next() {
                result = self.fs().root_inode().await;
                rest_path = String::from(rest_path[1..].trim_start_matches('/'));
                continue;
            }
            let name;
            match rest_path.find('/') {
                None => {
                    name = rest_path;
                    rest_path = String::new();
                }
                Some(pos) => {
                    name = String::from(&rest_path[0..pos]);
                    rest_path = String::from(rest_path[pos + 1..].trim_start_matches('/'));
                }
            };
            if name == "" {
                continue;
            }
            let inode = result.find(&name).await?;
            // Handle symlink
            if inode.metadata().await?.type_ == FileType::SymLink && max_follow_times > 0 {
                if follow_times >= max_follow_times {
                    return_errno!(ELOOP, "too many symlinks");
                }
                // result remains unchanged
                rest_path = {
                    let mut new_path = inode.read_link().await?;
                    if let Some('/') = new_path.chars().last() {
                        new_path += &rest_path;
                    } else {
                        new_path += "/";
                        new_path += &rest_path;
                    }
                    new_path
                };
                follow_times += 1;
            } else {
                result = inode
            }
        }
        Ok(result)
    }
}

impl dyn AsyncInode {
    /// Downcast the INode to specific struct
    pub fn downcast_ref<T: AsyncInode>(&self) -> Option<&T> {
        self.as_any_ref().downcast_ref::<T>()
    }
}

/// Abstract Async FileSystem
#[async_trait]
pub trait AsyncFileSystem: Sync + Send {
    /// Sync all data to the storage
    async fn sync(&self) -> Result<()>;

    /// Get the root INode of the file system
    async fn root_inode(&self) -> Arc<dyn AsyncInode>;

    /// Get the file system information
    async fn info(&self) -> FsInfo;
}
