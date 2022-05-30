use crate::prelude::*;
use crate::storage::{SFSPageAlloc, SFSStorage};
use crate::structs::*;
use crate::utils::{BlockRangeIter, Dirty};

use async_trait::async_trait;
use async_vfs::{AsyncFileSystem, AsyncInode};
use bitvec::prelude::*;
use block_device::BlockDevice;
use lru::LruCache;
use page_cache::CachedDisk;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::mem::MaybeUninit;
use std::{
    string::String,
    sync::{Arc, Weak},
    vec,
};

/// Inode for AsyncSimpleFS
pub struct SFSInode {
    /// Inner inode
    inner: AsyncRwLock<InodeInner>,
    /// Extensions for Inode, e.g., flock
    ext: Extension,
}

impl SFSInode {
    pub(crate) fn new(inner: InodeInner, ext: Extension) -> Self {
        Self {
            inner: AsyncRwLock::new(inner),
            ext,
        }
    }
}

impl Debug for SFSInode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SFSInode {:?}", self.inner)
    }
}

#[async_trait]
impl AsyncInode for SFSInode {
    async fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        let len = match self.metadata().await?.type_ {
            VfsFileType::File | VfsFileType::SymLink => {
                self.inner.read().await._read_at(offset, buf).await?
            }
            _ => return_errno!(EISDIR, "not file"),
        };
        Ok(len)
    }

    async fn write_at(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        let info = self.metadata().await?;
        let len = match info.type_ {
            VfsFileType::File | VfsFileType::SymLink => {
                let end_offset = offset + buf.len();
                if info.size < end_offset {
                    let mut inner_mut = self.inner.write().await;
                    inner_mut._resize(end_offset).await?;
                    inner_mut._write_at(offset, buf).await?
                } else {
                    self.inner.read().await._write_at(offset, buf).await?
                }
            }
            _ => return_errno!(EISDIR, "not file"),
        };
        Ok(len)
    }

    /// the size returned here is logical size(entry num for directory), not the disk space used.
    async fn metadata(&self) -> Result<Metadata> {
        let inner = self.inner.read().await;
        let disk_inode = &inner.disk_inode;
        Ok(Metadata {
            dev: 0,
            rdev: 0,
            inode: inner.id,
            size: match disk_inode.type_ {
                FileType::File | FileType::SymLink | FileType::Dir => disk_inode.size as usize,
                FileType::CharDevice => 0,
                FileType::BlockDevice => 0,
                _ => panic!("Unknown file type"),
            },
            mode: 0o777,
            type_: VfsFileType::from(disk_inode.type_.clone()),
            blocks: disk_inode.blocks as usize * (BLOCK_SIZE / 512), // Number of 512B blocks
            atime: Timespec { sec: 0, nsec: 0 },
            mtime: Timespec { sec: 0, nsec: 0 },
            ctime: Timespec { sec: 0, nsec: 0 },
            nlinks: disk_inode.nlinks as usize,
            uid: 0,
            gid: 0,
            blk_size: BLOCK_SIZE,
        })
    }

    async fn set_metadata(&self, _metadata: &Metadata) -> Result<()> {
        // No-op for sfs
        Ok(())
    }

    async fn sync_all(&self) -> Result<()> {
        if self.inner.read().await.dirty() {
            self.inner.write().await.sync_metadata().await?;
        }
        Ok(())
    }

    async fn sync_data(&self) -> Result<()> {
        // Do nothing?
        Ok(())
    }

    async fn resize(&self, len: usize) -> Result<()> {
        match self.metadata().await?.type_ {
            VfsFileType::File | VfsFileType::SymLink => {
                self.inner.write().await._resize(len).await?
            }
            _ => return_errno!(EISDIR, "not file"),
        }
        Ok(())
    }

    async fn create(
        &self,
        name: &str,
        type_: VfsFileType,
        _mode: u16,
    ) -> Result<Arc<dyn AsyncInode>> {
        let info = self.metadata().await?;
        if info.type_ != VfsFileType::Dir {
            return_errno!(ENOTDIR, "not dir");
        }
        if info.nlinks == 0 {
            return_errno!(ENOENT, "dir removed");
        }

        // Ensure the name is not exist
        if self
            .inner
            .read()
            .await
            .get_file_inode_id(name)
            .await
            .is_some()
        {
            return_errno!(EEXIST, "entry exist");
        }

        // Create new INode
        let inode = {
            let fs = self.inner.read().await.fs();
            match type_ {
                VfsFileType::File => fs.inner().new_inode_file().await?,
                VfsFileType::SymLink => fs.inner().new_inode_symlink().await?,
                VfsFileType::Dir => fs.inner().new_inode_dir(self.inner.read().await.id).await?,
                _ => return_errno!(EINVAL, "invalid type"),
            }
        };

        // Write new entry
        let (mut inner_mut, mut inode_inner_mut) = write_lock_two_inodes(self, &inode).await;
        inner_mut
            .append_direntry(&DiskEntry {
                id: inode_inner_mut.id as u32,
                name: Str256::from(name),
                type_: inode_inner_mut.disk_inode.type_,
            })
            .await?;
        inode_inner_mut.nlinks_inc();
        if type_ == VfsFileType::Dir {
            inode_inner_mut.nlinks_inc(); //for .
            inner_mut.nlinks_inc(); //for ..
        }

        Ok(inode.clone())
    }

    async fn link(&self, name: &str, other: &Arc<dyn AsyncInode>) -> Result<()> {
        let info = self.metadata().await?;
        if info.type_ != VfsFileType::Dir {
            return_errno!(ENOTDIR, "not dir");
        }
        if info.nlinks == 0 {
            return_errno!(ENOENT, "dir removed");
        }
        if !self
            .inner
            .read()
            .await
            .get_file_inode_id(name)
            .await
            .is_none()
        {
            return_errno!(EEXIST, "entry exist");
        }
        let child = other
            .downcast_ref::<SFSInode>()
            .ok_or(errno!(EXDEV, "not same fs"))?;
        if !Arc::ptr_eq(&self.fs().await, &child.fs().await) {
            return_errno!(EXDEV, "not same fs");
        }
        if child.metadata().await?.type_ == VfsFileType::Dir {
            return_errno!(EISDIR, "entry is dir");
        }
        let (mut self_inner_mut, mut other_inner_mut) = write_lock_two_inodes(self, child).await;
        self_inner_mut
            .append_direntry(&DiskEntry {
                id: other_inner_mut.id as u32,
                name: Str256::from(name),
                type_: other_inner_mut.disk_inode.type_,
            })
            .await?;
        other_inner_mut.nlinks_inc();
        Ok(())
    }

    async fn unlink(&self, name: &str) -> Result<()> {
        let info = self.metadata().await?;
        if info.type_ != VfsFileType::Dir {
            return_errno!(ENOTDIR, "not dir");
        }
        if info.nlinks == 0 {
            return_errno!(ENOENT, "dir removed");
        }
        if name == "." || name == ".." {
            return_errno!(EISDIR, "name is dir");
        }

        let (inode_id, type_, entry_id) = self
            .inner
            .read()
            .await
            .get_file_inode_and_entry_id(name)
            .await
            .ok_or(errno!(ENOENT, "not found"))?;
        let inode = self
            .inner
            .read()
            .await
            .fs()
            .inner()
            .get_inode(inode_id)
            .await;
        if type_ == FileType::Dir {
            // only . and ..
            if inode.metadata().await?.size / DIRENT_SIZE > 2 {
                return_errno!(ENOTEMPTY, "dir not empty");
            }
        }
        let (mut self_inner_mut, mut other_inner_mut) = write_lock_two_inodes(self, &inode).await;
        other_inner_mut.nlinks_dec();
        if type_ == FileType::Dir {
            other_inner_mut.nlinks_dec(); //for .
            self_inner_mut.nlinks_dec(); //for ..
        }
        self_inner_mut.remove_direntry(entry_id).await?;

        Ok(())
    }

    async fn move_(
        &self,
        old_name: &str,
        target: &Arc<dyn AsyncInode>,
        new_name: &str,
    ) -> Result<()> {
        let info = self.metadata().await?;
        if info.type_ != VfsFileType::Dir {
            return_errno!(ENOTDIR, "self not dir");
        }
        if info.nlinks == 0 {
            return_errno!(ENOENT, "dir removed");
        }
        if old_name == "." || old_name == ".." {
            return_errno!(EISDIR, "name is dir");
        }

        let dest = target
            .downcast_ref::<SFSInode>()
            .ok_or(errno!(EXDEV, "not same fs"))?;
        let dest_info = dest.metadata().await?;
        if !Arc::ptr_eq(&self.fs().await, &dest.fs().await) {
            return_errno!(EXDEV, "not same fs");
        }
        if dest_info.type_ != VfsFileType::Dir {
            return_errno!(ENOTDIR, "dest not dir");
        }
        if dest_info.nlinks == 0 {
            return_errno!(ENOENT, "dest dir removed");
        }

        let (inode_id, inode_type, entry_id) = self
            .inner
            .read()
            .await
            .get_file_inode_and_entry_id(old_name)
            .await
            .ok_or(errno!(ENOENT, "not found"))?;
        if let Ok(dest_inode) = dest.find(new_name).await {
            let info = dest_inode.metadata().await?;
            if inode_id == info.inode {
                return Ok(());
            }
            let old_type = VfsFileType::from(inode_type);
            let dest_type = info.type_;
            match (old_type, dest_type) {
                (VfsFileType::Dir, VfsFileType::Dir) => {
                    if info.size / DIRENT_SIZE != 2 {
                        return_errno!(ENOTEMPTY, "dir not empty");
                    }
                }
                (VfsFileType::Dir, _) => {
                    return_errno!(ENOTDIR, "not dir");
                }
                (_, VfsFileType::Dir) => {
                    return_errno!(EISDIR, "entry is dir");
                }
                _ => {}
            }
            dest.unlink(new_name).await?;
        }

        if info.inode == dest_info.inode {
            // rename: in place modify name
            self.inner
                .write()
                .await
                .write_direntry(
                    entry_id,
                    &DiskEntry {
                        id: inode_id as u32,
                        name: Str256::from(new_name),
                        type_: inode_type,
                    },
                )
                .await?;
        } else {
            // move
            let (mut self_inner_mut, mut dest_inner_mut) = write_lock_two_inodes(self, dest).await;
            dest_inner_mut
                .append_direntry(&DiskEntry {
                    id: inode_id as u32,
                    name: Str256::from(new_name),
                    type_: inode_type,
                })
                .await?;
            self_inner_mut.remove_direntry(entry_id).await?;

            let inode = self_inner_mut.fs().inner().get_inode(inode_id).await;
            if inode.metadata().await?.type_ == VfsFileType::Dir {
                self_inner_mut.nlinks_dec();
                dest_inner_mut.nlinks_inc();
            }
        }
        Ok(())
    }

    async fn find(&self, name: &str) -> Result<Arc<dyn AsyncInode>> {
        let info = self.metadata().await?;
        if info.type_ != VfsFileType::Dir {
            return_errno!(ENOTDIR, "not dir");
        }
        let self_inner = self.inner.read().await;
        let inode_id = self_inner
            .get_file_inode_id(name)
            .await
            .ok_or(errno!(ENOENT, "not found"))?;
        Ok(self_inner.fs().inner().get_inode(inode_id).await)
    }

    async fn read_link(&self) -> Result<String> {
        if self.metadata().await?.type_ != VfsFileType::SymLink {
            return_errno!(EINVAL, "not symlink");
        }
        let mut content = vec![0u8; PATH_MAX];
        let len = self.read_at(0, &mut content).await?;
        let path = std::str::from_utf8(&content[..len])
            .map_err(|_| errno!(ENOENT, "invalid symlink content"))?;
        Ok(String::from(path))
    }

    async fn write_link(&self, target: &str) -> Result<()> {
        if self.metadata().await?.type_ != VfsFileType::SymLink {
            return_errno!(EINVAL, "not symlink");
        }
        let data = target.as_bytes();
        let len = self.write_at(0, data).await?;
        debug_assert!(len == data.len());
        Ok(())
    }

    async fn get_entry(&self, id: usize) -> Result<String> {
        let info = self.metadata().await?;
        if info.type_ != VfsFileType::Dir {
            return_errno!(ENOTDIR, "not dir");
        }
        if id >= info.size as usize / DIRENT_SIZE {
            return_errno!(ENOENT, "can not find");
        };
        let entry = self.inner.read().await.read_direntry(id).await?;
        Ok(String::from(entry.name.as_ref()))
    }

    async fn iterate_entries(&self, ctx: &mut DirentWriterContext) -> Result<usize> {
        if self.metadata().await?.type_ != VfsFileType::Dir {
            return_errno!(ENOTDIR, "not dir");
        }

        let mut total_written_len = 0;
        let inner = self.inner.read().await;
        for entry_id in ctx.pos()..inner.disk_inode.size as usize / DIRENT_SIZE {
            let entry = inner.read_direntry(entry_id).await?;
            let written_len = match ctx.write_entry(
                entry.name.as_ref(),
                entry.id as u64,
                VfsFileType::from(entry.type_),
            ) {
                Ok(written_len) => written_len,
                Err(_) => {
                    if total_written_len == 0 {
                        return_errno!(EINVAL, "write entry fail");
                    } else {
                        break;
                    }
                }
            };
            total_written_len += written_len;
        }
        Ok(total_written_len)
    }

    fn ioctl(&self, _cmd: &mut dyn IoctlCmd) -> Result<()> {
        return_errno!(ENOSYS, "not support ioctl");
    }

    async fn fs(&self) -> Arc<dyn AsyncFileSystem> {
        self.inner.read().await.fs()
    }

    fn ext(&self) -> Option<&Extension> {
        Some(&self.ext)
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl Drop for SFSInode {
    fn drop(&mut self) {
        // do nothing
    }
}

async fn write_lock_two_inodes<'a>(
    this: &'a SFSInode,
    other: &'a SFSInode,
) -> (
    AsyncRwLockWriteGuard<'a, InodeInner>,
    AsyncRwLockWriteGuard<'a, InodeInner>,
) {
    if this.inner.read().await.id < other.inner.read().await.id {
        let this = this.inner.write().await;
        let other = other.inner.write().await;
        (this, other)
    } else {
        let other = other.inner.write().await;
        let this = this.inner.write().await;
        (this, other)
    }
}

/// Inner inode for AsyncSimpleFS
pub(crate) struct InodeInner {
    /// INode number
    id: InodeId,
    /// On-disk INode
    disk_inode: Dirty<DiskInode>,
    /// Reference to SFS, used by almost all operations
    fs: Weak<AsyncSimpleFS>,
}

impl Debug for InodeInner {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "InodeInner {{ id: {}, disk: {:?} }}",
            self.id, self.disk_inode
        )
    }
}

impl InodeInner {
    fn fs(&self) -> Arc<AsyncSimpleFS> {
        self.fs.upgrade().unwrap()
    }

    /// Map file block id to device block id
    async fn get_device_block_id(&self, file_block_id: BlockId) -> Result<BlockId> {
        let disk_inode = &self.disk_inode;
        let device_block_id = match file_block_id {
            id if id >= disk_inode.blocks as BlockId => {
                return_errno!(EINVAL, "invalid file block id")
            }
            id if id < MAX_NBLOCK_DIRECT => disk_inode.direct[id],
            id if id < MAX_NBLOCK_INDIRECT => {
                let device_block_id = self
                    .fs()
                    .inner()
                    .storage
                    .load_struct::<u32>(disk_inode.indirect as BlockId, ENTRY_SIZE * (id - NDIRECT))
                    .await?;
                device_block_id
            }
            id if id < MAX_NBLOCK_DOUBLE_INDIRECT => {
                // double indirect
                let indirect_id = id - MAX_NBLOCK_INDIRECT;
                let indirect_block_id = self
                    .fs()
                    .inner()
                    .storage
                    .load_struct::<u32>(
                        disk_inode.db_indirect as BlockId,
                        ENTRY_SIZE * (indirect_id / BLK_NENTRY),
                    )
                    .await?;
                assert!(indirect_block_id > 0);
                let device_block_id = self
                    .fs()
                    .inner()
                    .storage
                    .load_struct::<u32>(
                        indirect_block_id as BlockId,
                        ENTRY_SIZE * (indirect_id as usize % BLK_NENTRY),
                    )
                    .await?;
                assert!(device_block_id > 0);
                device_block_id
            }
            _ => unimplemented!("triple indirect blocks is not supported"),
        };
        Ok(device_block_id as BlockId)
    }

    /// Set the device block id for the file block id
    async fn set_device_block_id(
        &mut self,
        file_block_id: BlockId,
        device_block_id: BlockId,
    ) -> Result<()> {
        match file_block_id {
            id if id >= self.disk_inode.blocks as BlockId => {
                return_errno!(EINVAL, "invalid file block id")
            }
            id if id < MAX_NBLOCK_DIRECT => {
                self.disk_inode.direct[id] = device_block_id as u32;
                Ok(())
            }
            id if id < MAX_NBLOCK_INDIRECT => {
                let device_block_id = device_block_id as u32;
                self.fs()
                    .inner()
                    .storage
                    .store_struct::<u32>(
                        self.disk_inode.indirect as BlockId,
                        ENTRY_SIZE * (id - NDIRECT),
                        &device_block_id,
                    )
                    .await?;
                Ok(())
            }
            id if id < MAX_NBLOCK_DOUBLE_INDIRECT => {
                // double indirect
                let indirect_id = id - MAX_NBLOCK_INDIRECT;
                let indirect_block_id = self
                    .fs()
                    .inner()
                    .storage
                    .load_struct::<u32>(
                        self.disk_inode.db_indirect as BlockId,
                        ENTRY_SIZE * (indirect_id / BLK_NENTRY),
                    )
                    .await?;
                assert!(indirect_block_id > 0);
                let device_block_id = device_block_id as u32;
                self.fs()
                    .inner()
                    .storage
                    .store_struct::<u32>(
                        indirect_block_id as BlockId,
                        ENTRY_SIZE * (indirect_id as usize % BLK_NENTRY),
                        &device_block_id,
                    )
                    .await?;
                Ok(())
            }
            _ => unimplemented!("triple indirect blocks is not supported"),
        }
    }

    /// Only for Dir
    async fn get_file_inode_and_entry_id(&self, name: &str) -> Option<(InodeId, FileType, usize)> {
        for i in 0..self.disk_inode.size as usize / DIRENT_SIZE {
            let entry = self.read_direntry(i).await.unwrap();
            if entry.name.as_ref() == name {
                return Some((entry.id as InodeId, entry.type_, i));
            }
        }
        None
    }

    async fn get_file_inode_id(&self, name: &str) -> Option<InodeId> {
        self.get_file_inode_and_entry_id(name)
            .await
            .map(|(inode_id, _, _)| inode_id)
    }

    /// Init dir content. Insert 2 init entries.
    /// This do not init nlinks, please modify the nlinks in the invoker.
    async fn init_direntry(&mut self, parent: InodeId) -> Result<()> {
        // Insert entries: '.' '..'
        self._resize(DIRENT_SIZE * 2).await?;
        self.write_direntry(
            0,
            &DiskEntry {
                id: self.id as u32,
                name: Str256::from("."),
                type_: FileType::Dir,
            },
        )
        .await?;
        self.write_direntry(
            1,
            &DiskEntry {
                id: parent as u32,
                name: Str256::from(".."),
                type_: FileType::Dir,
            },
        )
        .await?;
        Ok(())
    }

    async fn read_direntry(&self, id: usize) -> Result<DiskEntry> {
        let mut direntry: DiskEntry = unsafe { MaybeUninit::uninit().assume_init() };
        self._read_at(DIRENT_SIZE * id, direntry.as_buf_mut())
            .await?;
        Ok(direntry)
    }

    async fn write_direntry(&mut self, id: usize, direntry: &DiskEntry) -> Result<()> {
        self._write_at(DIRENT_SIZE * id, direntry.as_buf()).await?;
        Ok(())
    }

    async fn append_direntry(&mut self, direntry: &DiskEntry) -> Result<()> {
        let size = self.disk_inode.size as usize;
        let dirent_count = size / DIRENT_SIZE;
        self._resize(size + DIRENT_SIZE).await?;
        self.write_direntry(dirent_count, direntry).await?;
        Ok(())
    }

    /// remove a direntry in middle of file and insert the last one here, useful for direntry remove
    /// should be only used in unlink
    async fn remove_direntry(&mut self, id: usize) -> Result<()> {
        let size = self.disk_inode.size as usize;
        let dirent_count = size / DIRENT_SIZE;
        debug_assert!(id < dirent_count);
        let last_dirent = self.read_direntry(dirent_count - 1).await?;
        self.write_direntry(id, &last_dirent).await?;
        self._resize(size - DIRENT_SIZE).await?;
        Ok(())
    }

    /// Resize content size, no matter what type it is
    async fn _resize(&mut self, len: usize) -> Result<()> {
        if len > MAX_FILE_SIZE {
            return_errno!(EINVAL, "size too big");
        }
        let blocks = (len + BLOCK_SIZE - 1) / BLOCK_SIZE;
        if blocks > MAX_NBLOCK_DOUBLE_INDIRECT {
            return_errno!(EINVAL, "size too big");
        }
        use core::cmp::Ordering;
        let old_blocks = self.disk_inode.blocks as usize;
        match blocks.cmp(&old_blocks) {
            Ordering::Equal => {
                self.disk_inode.size = len as u32;
            }
            Ordering::Greater => {
                self.disk_inode.blocks = blocks as u32;
                // allocate indirect block if needed
                if old_blocks < MAX_NBLOCK_DIRECT && blocks >= MAX_NBLOCK_DIRECT {
                    self.disk_inode.indirect =
                        self.fs().inner().alloc_block().await.expect("no space") as u32;
                }
                // allocate double indirect block if needed
                if blocks >= MAX_NBLOCK_INDIRECT {
                    if self.disk_inode.db_indirect == 0 {
                        self.disk_inode.db_indirect =
                            self.fs().inner().alloc_block().await.expect("no space") as u32;
                    }
                    let indirect_begin = {
                        if old_blocks < MAX_NBLOCK_INDIRECT {
                            0
                        } else {
                            (old_blocks - MAX_NBLOCK_INDIRECT) / BLK_NENTRY + 1
                        }
                    };
                    let indirect_end = (blocks - MAX_NBLOCK_INDIRECT) / BLK_NENTRY + 1;
                    for i in indirect_begin..indirect_end {
                        let indirect =
                            self.fs().inner().alloc_block().await.expect("no space") as u32;
                        self.fs()
                            .inner()
                            .storage
                            .store_struct::<u32>(
                                self.disk_inode.db_indirect as BlockId,
                                ENTRY_SIZE * i,
                                &indirect,
                            )
                            .await?;
                    }
                }
                // allocate extra blocks
                for file_block_id in old_blocks..blocks {
                    let device_block_id = self.fs().inner().alloc_block().await.expect("no space");
                    self.set_device_block_id(file_block_id, device_block_id)
                        .await?;
                }
                self.disk_inode.size = len as u32;
            }
            Ordering::Less => {
                // free extra blocks
                for file_block_id in blocks..old_blocks {
                    let device_block_id = self.get_device_block_id(file_block_id).await?;
                    self.fs().inner().free_block(device_block_id).await?;
                }
                // free indirect block if needed
                if blocks < MAX_NBLOCK_DIRECT && old_blocks >= MAX_NBLOCK_DIRECT {
                    self.fs()
                        .inner()
                        .free_block(self.disk_inode.indirect as BlockId)
                        .await?;
                    self.disk_inode.indirect = 0;
                }
                // free double indirect block if needed
                if old_blocks >= MAX_NBLOCK_INDIRECT {
                    let indirect_begin = {
                        if blocks < MAX_NBLOCK_INDIRECT {
                            0
                        } else {
                            (blocks - MAX_NBLOCK_INDIRECT) / BLK_NENTRY + 1
                        }
                    };
                    let indirect_end = (old_blocks - MAX_NBLOCK_INDIRECT) / BLK_NENTRY + 1;
                    for i in indirect_begin..indirect_end {
                        let indirect = self
                            .fs()
                            .inner()
                            .storage
                            .load_struct::<u32>(
                                self.disk_inode.db_indirect as BlockId,
                                ENTRY_SIZE * i,
                            )
                            .await?;
                        assert!(indirect > 0);
                        self.fs().inner().free_block(indirect as BlockId).await?;
                    }
                    if blocks < MAX_NBLOCK_INDIRECT {
                        assert!(self.disk_inode.db_indirect > 0);
                        self.fs()
                            .inner()
                            .free_block(self.disk_inode.db_indirect as BlockId)
                            .await?;
                        self.disk_inode.db_indirect = 0;
                    }
                }
                self.disk_inode.blocks = blocks as u32;
                self.disk_inode.size = len as u32;
            }
        }
        Ok(())
    }

    /// Read content, no matter what type it is
    async fn _read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        let file_size = self.disk_inode.size as usize;
        let iter = BlockRangeIter {
            begin: file_size.min(offset),
            end: file_size.min(offset + buf.len()),
            block_size: BLOCK_SIZE,
        };

        let mut read_len = 0;
        for range in iter {
            let device_block_id = self.get_device_block_id(range.block_id).await?;
            let len = self
                .fs()
                .inner()
                .storage
                .read_at(
                    device_block_id,
                    &mut buf[read_len..read_len + range.len()],
                    range.begin,
                )
                .await?;
            debug_assert!(len == range.len());
            read_len += len;
        }

        Ok(read_len)
    }

    /// Write content, no matter what type it is
    async fn _write_at(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        let file_size = self.disk_inode.size as usize;
        let iter = BlockRangeIter {
            begin: file_size.min(offset),
            end: file_size.min(offset + buf.len()),
            block_size: BLOCK_SIZE,
        };

        let mut write_len = 0;
        for range in iter {
            let device_block_id = self.get_device_block_id(range.block_id).await?;
            let len = self
                .fs()
                .inner()
                .storage
                .write_at(
                    device_block_id,
                    &buf[write_len..write_len + range.len()],
                    range.begin,
                )
                .await?;
            debug_assert!(len == range.len());
            write_len += len;
        }

        Ok(write_len)
    }

    fn nlinks_inc(&mut self) {
        self.disk_inode.nlinks += 1;
    }

    fn nlinks_dec(&mut self) {
        assert!(self.disk_inode.nlinks > 0);
        self.disk_inode.nlinks -= 1;
    }

    fn dirty(&self) -> bool {
        self.disk_inode.dirty()
    }

    async fn sync_metadata(&mut self) -> Result<()> {
        if self.disk_inode.nlinks == 0 {
            self._resize(0).await?;
            self.disk_inode.sync();
            self.fs().inner().free_block(self.id).await?;
            return Ok(());
        }
        self.fs()
            .inner()
            .storage
            .store_struct::<DiskInode>(self.id, 0, &self.disk_inode)
            .await?;
        self.disk_inode.sync();
        Ok(())
    }
}

/// Async Simple Filesystem
pub struct AsyncSimpleFS(Option<FsInner>);

impl AsyncSimpleFS {
    /// Load SFS from the existing block device
    pub async fn open(device: Arc<dyn BlockDevice>) -> Result<Arc<Self>> {
        let device_storage = SFSStorage::from_device(device.clone());
        // Load the superblock
        let super_block = device_storage
            .load_struct::<SuperBlock>(BLKN_SUPER, 0)
            .await?;
        if !super_block.check() {
            return_errno!(EINVAL, "wrong fs super block");
        }
        // Load the freemap
        let mut freemap_disk = vec![0u8; BLOCK_SIZE * super_block.freemap_blocks as usize];
        device_storage
            .read_at(BLKN_FREEMAP, freemap_disk.as_mut_slice(), 0)
            .await?;

        Ok(Self(Some(FsInner {
            super_block: AsyncRwLock::new(Dirty::new(super_block)),
            free_map: AsyncRwLock::new(Dirty::new(BitVec::from(freemap_disk.as_slice()))),
            inodes: AsyncRwLock::new(LruCache::new(INODE_CACHE_SIZE)),
            #[cfg(feature = "pagecache")]
            storage: SFSStorage::from_page_cache(CachedDisk::<SFSPageAlloc>::new(device).unwrap()),
            #[cfg(not(feature = "pagecache"))]
            storage: SFSStorage::from_device(device),
            self_ptr: Weak::default(),
        }))
        .wrap())
    }

    /// Create a new SFS on blank disk
    pub async fn create(device: Arc<dyn BlockDevice>) -> Result<Arc<Self>> {
        let space = device.total_bytes();
        let blocks = (space + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let freemap_blocks = (space + BLKBITS * BLOCK_SIZE - 1) / BLKBITS / BLOCK_SIZE;
        assert!(blocks >= 16, "space too small");

        let super_block = SuperBlock {
            magic: SFS_MAGIC,
            blocks: blocks as u32,
            unused_blocks: (blocks - BLKN_FREEMAP - freemap_blocks) as u32,
            info: Str32::from(DEFAULT_INFO),
            freemap_blocks: freemap_blocks as u32,
        };
        let free_map = {
            let mut bitset = BitVec::with_capacity(freemap_blocks * BLKBITS);
            bitset.extend(core::iter::repeat(false).take(freemap_blocks * BLKBITS));
            for i in (BLKN_FREEMAP + freemap_blocks)..blocks {
                bitset.set(i, true);
            }
            bitset
        };

        let sfs = Self(Some(FsInner {
            super_block: AsyncRwLock::new(Dirty::new_dirty(super_block)),
            free_map: AsyncRwLock::new(Dirty::new_dirty(free_map)),
            inodes: AsyncRwLock::new(LruCache::new(INODE_CACHE_SIZE)),
            #[cfg(feature = "pagecache")]
            storage: SFSStorage::from_page_cache(CachedDisk::<SFSPageAlloc>::new(device).unwrap()),
            #[cfg(not(feature = "pagecache"))]
            storage: SFSStorage::from_device(device),
            self_ptr: Weak::default(),
        }))
        .wrap();

        // Init root INode
        let root = sfs
            .inner()
            ._new_inode(BLKN_ROOT, Dirty::new_dirty(DiskInode::new_dir()))
            .await;
        let mut root_inner_mut = root.inner.write().await;
        root_inner_mut.init_direntry(BLKN_ROOT).await?;
        root_inner_mut.nlinks_inc(); //for .
        root_inner_mut.nlinks_inc(); //for ..(root's parent is itself)
        drop(root_inner_mut);
        root.sync_all().await?;
        sfs.inner().sync_metadata().await?;

        Ok(sfs)
    }

    /// Wrap pure AsyncSimpleFS with Arc
    /// Private used in constructors
    fn wrap(self) -> Arc<Self> {
        // Create an Arc, make a Weak from it, then put it into the struct.
        // It's a little tricky.
        let fs = Arc::new(self);
        let weak = Arc::downgrade(&fs);
        let ptr = Arc::into_raw(fs) as *mut Self;
        unsafe {
            (*ptr).0.as_mut().unwrap().self_ptr = weak;
        }
        unsafe { Arc::from_raw(ptr) }
    }

    pub(crate) fn inner(&self) -> &FsInner {
        debug_assert!(self.0.is_some());
        self.0.as_ref().unwrap()
    }
}

#[async_trait]
impl AsyncFileSystem for AsyncSimpleFS {
    async fn sync(&self) -> Result<()> {
        Ok(self.inner().sync_all().await?)
    }

    async fn root_inode(&self) -> Arc<dyn AsyncInode> {
        let inode = self.inner().get_inode(BLKN_ROOT).await;
        inode
    }

    async fn info(&self) -> FsInfo {
        let sb = self.inner().super_block.read().await;
        FsInfo {
            magic: sb.magic as usize,
            bsize: BLOCK_SIZE,
            frsize: BLOCK_SIZE,
            blocks: sb.blocks as usize,
            bfree: sb.unused_blocks as usize,
            bavail: sb.unused_blocks as usize,
            files: sb.blocks as usize,        // inaccurate
            ffree: sb.unused_blocks as usize, // inaccurate
            namemax: MAX_FNAME_LEN,
        }
    }
}

impl Drop for AsyncSimpleFS {
    /// Auto sync when drop
    fn drop(&mut self) {
        // do nothing
        // let fs_inner = self.0.take().unwrap();
        // async_rt::task::spawn(async move {
        //     fs_inner
        //         .sync_all()
        //         .await
        //         .expect("Failed to sync when dropping the AsyncSimpleFS");
        // });
    }
}

/// Inner for AsyncSimpleFS
pub(crate) struct FsInner {
    /// on-disk superblock
    super_block: AsyncRwLock<Dirty<SuperBlock>>,
    /// described the usage of blocks, the blocks in use are marked 0
    free_map: AsyncRwLock<Dirty<FreeMap>>,
    /// cached inodes
    inodes: AsyncRwLock<LruCache<InodeId, Arc<SFSInode>>>,
    /// underlying storage
    storage: SFSStorage,
    /// pointer to self, used by inodes
    self_ptr: Weak<AsyncSimpleFS>,
}

impl FsInner {
    /// Allocate a free block, return block id
    async fn alloc_block(&self) -> Option<BlockId> {
        let mut free_map = self.free_map.write().await;
        let id = free_map.alloc();
        if let Some(block_id) = id {
            let mut super_block = self.super_block.write().await;
            if super_block.unused_blocks == 0 {
                free_map.set(block_id, true);
                return None;
            }
            // will not underflow
            super_block.unused_blocks -= 1;
            //trace!("alloc block {:#x}", block_id);
        } else {
            let super_block = self.super_block.read().await;
            panic!("failed to allocate block: {:?}", *super_block)
        }
        id
    }

    /// Free a block
    async fn free_block(&self, block_id: BlockId) -> Result<()> {
        let mut free_map = self.free_map.write().await;
        let mut super_block = self.super_block.write().await;
        assert!(!free_map[block_id]);
        free_map.set(block_id, true);
        super_block.unused_blocks += 1;
        //trace!("free block {:#x}", block_id);
        // clean the block after free
        static ZEROS: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
        self.storage.write_at(block_id, &ZEROS, 0).await?;
        Ok(())
    }

    /// Create a new inode struct, then insert it to inode caches
    /// Private used for load or create inode
    async fn _new_inode(&self, id: InodeId, disk_inode: Dirty<DiskInode>) -> Arc<SFSInode> {
        let inode = {
            let inode_inner = InodeInner {
                id,
                disk_inode,
                fs: self.self_ptr.clone(),
            };
            Arc::new(SFSInode::new(inode_inner, Extension::new()))
        };
        if let Some((_, lru_inode)) = self.inodes.write().await.push(id, inode.clone()) {
            lru_inode.sync_all().await.unwrap();
        }
        inode
    }

    /// Get inode by id. Load if not in memory.
    /// ** Must ensure it's a valid INode **
    async fn get_inode(&self, id: InodeId) -> Arc<SFSInode> {
        assert!(!self.free_map.read().await[id]);

        // In the cache
        if let Some(inode) = self.inodes.write().await.get(&id) {
            return inode.clone();
        }
        // Load if not in cache
        let disk_inode = self.storage.load_struct::<DiskInode>(id, 0).await.unwrap();
        self._new_inode(id, Dirty::new(disk_inode)).await
    }

    /// Create a new INode file
    async fn new_inode_file(&self) -> Result<Arc<SFSInode>> {
        let id = self
            .alloc_block()
            .await
            .ok_or(errno!(EIO, "no device space"))?;
        let disk_inode = Dirty::new_dirty(DiskInode::new_file());
        Ok(self._new_inode(id, disk_inode).await)
    }

    /// Create a new INode symlink
    async fn new_inode_symlink(&self) -> Result<Arc<SFSInode>> {
        let id = self
            .alloc_block()
            .await
            .ok_or(errno!(EIO, "no device space"))?;
        let disk_inode = Dirty::new_dirty(DiskInode::new_symlink());
        Ok(self._new_inode(id, disk_inode).await)
    }

    /// Create a new INode dir
    async fn new_inode_dir(&self, parent: InodeId) -> Result<Arc<SFSInode>> {
        let id = self
            .alloc_block()
            .await
            .ok_or(errno!(EIO, "no device space"))?;
        let disk_inode = Dirty::new_dirty(DiskInode::new_dir());
        let inode = self._new_inode(id, disk_inode).await;
        inode.inner.write().await.init_direntry(parent).await?;
        Ok(inode)
    }

    async fn sync_metadata(&self) -> Result<()> {
        let free_map_dirty = self.free_map.read().await.dirty();
        let super_block_dirty = self.super_block.read().await.dirty();
        if free_map_dirty {
            let mut free_map = self.free_map.write().await;
            self.storage
                .write_at(BLKN_FREEMAP, free_map.as_buf(), 0)
                .await?;
            free_map.sync();
        }
        if super_block_dirty {
            let mut super_block = self.super_block.write().await;
            self.storage
                .store_struct::<SuperBlock>(BLKN_SUPER, 0, &super_block)
                .await?;
            super_block.sync();
        }
        Ok(())
    }

    async fn sync_cached_inodes(&self) -> Result<()> {
        let mut inodes_map = self.inodes.write().await;
        let cnt = inodes_map.len();
        for _ in 0..cnt {
            let (_, inode) = inodes_map.pop_lru().unwrap();
            inode.sync_all().await?;
        }
        Ok(())
    }

    async fn sync_all(&self) -> Result<()> {
        // writeback cached inodes
        self.sync_cached_inodes().await?;
        // writeback freemap and superblock
        self.sync_metadata().await?;
        // flush to device
        self.storage.flush().await?;
        Ok(())
    }
}
