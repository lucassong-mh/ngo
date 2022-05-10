//! On-disk structures in SFS
use crate::prelude::*;

use static_assertions::const_assert;
use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::{slice, str};

/// On-disk superblock
#[repr(C)]
#[derive(Clone, Debug)]
pub struct SuperBlock {
    /// magic number, should be SFS_MAGIC
    pub magic: u32,
    /// number of blocks in fs
    pub blocks: u32,
    /// number of unused blocks in fs
    pub unused_blocks: u32,
    /// information for sfs
    pub info: Str32,
    /// number of freemap blocks
    pub freemap_blocks: u32,
}

/// inode (on disk)
#[repr(C)]
#[derive(Clone, Debug)]
pub struct DiskInode {
    /// size of the file (in bytes)
    /// undefined in dir (256 * #entries ?)
    pub size: u32,
    /// one of SYS_TYPE_* above
    pub type_: FileType,
    /// number of hard links to this file
    /// Note: "." and ".." is counted in this nlinks
    pub nlinks: u16,
    /// number of blocks
    pub blocks: u32,
    /// direct blocks
    pub direct: [u32; NDIRECT],
    /// indirect blocks
    pub indirect: u32,
    /// double indirect blocks
    pub db_indirect: u32,
}

#[repr(C)]
pub struct IndirectBlock {
    pub entries: [u32; BLK_NENTRY],
}

/// file entry (on disk)
#[repr(C)]
#[derive(Clone, Debug)]
pub struct DiskEntry {
    /// inode number
    pub id: u32,
    /// file name
    pub name: Str256,
}

/// file type
#[repr(u16)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum FileType {
    Invalid = 0,
    File = 1,
    Dir = 2,
    SymLink = 3,
    CharDevice = 4,
    BlockDevice = 5,
}

impl From<FileType> for VfsFileType {
    fn from(t: FileType) -> Self {
        match t {
            FileType::File => VfsFileType::File,
            FileType::SymLink => VfsFileType::SymLink,
            FileType::Dir => VfsFileType::Dir,
            FileType::CharDevice => VfsFileType::CharDevice,
            FileType::BlockDevice => VfsFileType::BlockDevice,
            _ => panic!("unknown file type"),
        }
    }
}

#[repr(C)]
#[derive(Clone)]
pub struct Str256(pub [u8; 256]);

#[repr(C)]
#[derive(Clone)]
pub struct Str32(pub [u8; 32]);

impl AsRef<str> for Str256 {
    fn as_ref(&self) -> &str {
        let len = self.0.iter().enumerate().find(|(_, &b)| b == 0).unwrap().0;
        str::from_utf8(&self.0[0..len]).unwrap()
    }
}

impl AsRef<str> for Str32 {
    fn as_ref(&self) -> &str {
        let len = self.0.iter().enumerate().find(|(_, &b)| b == 0).unwrap().0;
        str::from_utf8(&self.0[0..len]).unwrap()
    }
}

impl Debug for Str256 {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl Debug for Str32 {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl<'a> From<&'a str> for Str256 {
    fn from(s: &'a str) -> Self {
        let mut ret = [0u8; 256];
        ret[0..s.len()].copy_from_slice(s.as_ref());
        Str256(ret)
    }
}

impl<'a> From<&'a str> for Str32 {
    fn from(s: &'a str) -> Self {
        let mut ret = [0u8; 32];
        ret[0..s.len()].copy_from_slice(s.as_ref());
        Str32(ret)
    }
}

impl SuperBlock {
    pub fn check(&self) -> bool {
        self.magic == SFS_MAGIC
    }
}

impl DiskInode {
    pub const fn new_file() -> Self {
        DiskInode {
            size: 0,
            type_: FileType::File,
            nlinks: 0,
            blocks: 0,
            direct: [0; NDIRECT],
            indirect: 0,
            db_indirect: 0,
        }
    }
    pub const fn new_symlink() -> Self {
        DiskInode {
            size: 0,
            type_: FileType::SymLink,
            nlinks: 0,
            blocks: 0,
            direct: [0; NDIRECT],
            indirect: 0,
            db_indirect: 0,
        }
    }
    pub const fn new_dir() -> Self {
        DiskInode {
            size: 0,
            type_: FileType::Dir,
            nlinks: 0,
            blocks: 0,
            direct: [0; NDIRECT],
            indirect: 0,
            db_indirect: 0,
        }
    }
}

/// Convert structs to [u8] slice
pub trait AsBuf {
    fn as_buf(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const _ as *const u8, size_of_val(self)) }
    }
    fn as_buf_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self as *mut _ as *mut u8, size_of_val(self)) }
    }
}

impl AsBuf for SuperBlock {}

impl AsBuf for DiskInode {}

impl AsBuf for DiskEntry {}

impl AsBuf for u32 {}

impl AsBuf for [u8; BLOCK_SIZE] {}

pub type InodeId = BlockId;

/// magic number for sfs
pub const SFS_MAGIC: u32 = 0x2f8d_be2b;
/// number of direct blocks in inode
pub const NDIRECT: usize = 12;
/// default sfs infomation string
pub const DEFAULT_INFO: &str = "simple file system";
/// max length of infomation
pub const MAX_INFO_LEN: usize = 31;
/// max length of filename
pub const MAX_FNAME_LEN: usize = 255;
/// max file size in theory (48KB + 4MB + 4GB)
/// however, the file size is stored in u32
pub const MAX_FILE_SIZE: usize = 0xffffffff;
/// block the superblock lives in
pub const BLKN_SUPER: BlockId = 0;
/// location of the root dir inode
pub const BLKN_ROOT: BlockId = 1;
/// 1st block of the freemap
pub const BLKN_FREEMAP: BlockId = 2;
/// number of bits in a block
pub const BLKBITS: usize = BLOCK_SIZE * 8;
/// size of one entry
pub const ENTRY_SIZE: usize = 4;
/// number of entries in a block
pub const BLK_NENTRY: usize = BLOCK_SIZE / ENTRY_SIZE;
/// size of a dirent used in the size field
pub const DIRENT_SIZE: usize = MAX_FNAME_LEN + 1 + ENTRY_SIZE;
/// max number of blocks with direct blocks
pub const MAX_NBLOCK_DIRECT: usize = NDIRECT;
/// max number of blocks with indirect blocks
pub const MAX_NBLOCK_INDIRECT: usize = NDIRECT + BLK_NENTRY;
/// max number of blocks with double indirect blocks
pub const MAX_NBLOCK_DOUBLE_INDIRECT: usize = NDIRECT + BLK_NENTRY + BLK_NENTRY * BLK_NENTRY;

const_assert!(o1; size_of::<SuperBlock>() <= BLOCK_SIZE);
const_assert!(o2; size_of::<DiskInode>() <= BLOCK_SIZE);
const_assert!(o3; size_of::<DiskEntry>() <= BLOCK_SIZE);
const_assert!(o4; size_of::<IndirectBlock>() == BLOCK_SIZE);
const_assert!(o5; DEFAULT_INFO.len() <= MAX_INFO_LEN);
