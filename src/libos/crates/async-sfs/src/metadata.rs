//! On-disk metadata structures in fs
use crate::prelude::*;
use crate::utils::AsBuf;

use bitvec::prelude::*;
use static_assertions::const_assert;
use std::fmt::{Debug, Formatter};
use std::mem::size_of;
use std::str;

/// On-disk superblock
#[repr(C)]
#[derive(Clone, Debug)]
pub struct SuperBlock {
    /// magic number
    pub magic: u32,
    /// number of blocks in fs
    pub blocks: u32,
    /// number of unused blocks in fs
    pub unused_blocks: u32,
    /// information for fs
    pub info: Str32,
    /// number of freemap blocks
    pub freemap_blocks: u32,
}

/// On-disk freemap bitset
pub type FreeMapBitSet = BitVec<Lsb0, u8>;

/// Described the usage of blocks in bitset.
/// The bit is true means block is allocated and vice versa.
#[derive(Clone, Debug)]
pub struct FreeMap {
    bitset: FreeMapBitSet,
    first_unused_id: Option<usize>,
}

impl FreeMap {
    pub fn from_bitset(bitset: FreeMapBitSet) -> Self {
        let first_unused_id = (0..bitset.len()).find(|&i| !bitset[i]);
        Self {
            bitset,
            first_unused_id,
        }
    }

    pub fn alloc(&mut self) -> Option<usize> {
        if let Some(alloc_id) = self.first_unused_id {
            self.bitset.set(alloc_id, true);
            let next_unused_id = (alloc_id + 1..self.bitset.len()).find(|&i| !self.bitset[i]);
            self.first_unused_id = next_unused_id;
            Some(alloc_id)
        } else {
            None
        }
    }

    pub fn free(&mut self, id: usize) {
        self.bitset.set(id, false);
        match self.first_unused_id {
            Some(old_id) if id < old_id => {
                self.first_unused_id = Some(id);
            }
            Some(_) => {}
            None => {
                self.first_unused_id = Some(id);
            }
        }
    }

    pub fn is_allocated(&self, id: usize) -> bool {
        self.bitset[id]
    }
}

/// inode (on disk)
#[repr(C)]
#[derive(Clone, Debug)]
pub struct DiskInode {
    /// size of the file (in bytes)
    pub size: u32,
    /// type of the file
    pub type_: FileType,
    /// number of hard links to this file.
    /// Note: "." and ".." is counted in this nlinks
    pub nlinks: u32,
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
    /// file type
    pub type_: FileType,
}

/// file type
#[repr(u32)]
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

impl From<VfsFileType> for FileType {
    fn from(t: VfsFileType) -> Self {
        match t {
            VfsFileType::File => FileType::File,
            VfsFileType::SymLink => FileType::SymLink,
            VfsFileType::Dir => FileType::Dir,
            VfsFileType::CharDevice => FileType::CharDevice,
            VfsFileType::BlockDevice => FileType::BlockDevice,
            _ => FileType::Invalid,
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
            nlinks: 1,
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
            nlinks: 1,
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
            nlinks: 2,
            blocks: 0,
            direct: [0; NDIRECT],
            indirect: 0,
            db_indirect: 0,
        }
    }
}

impl AsBuf for SuperBlock {}

impl AsBuf for DiskInode {}

impl AsBuf for DiskEntry {}

impl AsBuf for u32 {}

impl AsBuf for [u8; BLOCK_SIZE] {}

impl AsBuf for FreeMap {
    fn as_buf(&self) -> &[u8] {
        self.bitset.as_ref()
    }
    fn as_buf_mut(&mut self) -> &mut [u8] {
        self.bitset.as_mut()
    }
}

pub type InodeId = BlockId;

/// magic number for sfs
pub const SFS_MAGIC: u32 = 0x2f8d_be2b;
/// number of max cached inodes, it is a soft limit
pub const INODE_CACHE_CAP: usize = 256;
/// number of direct blocks in inode
pub const NDIRECT: usize = 12;
/// default sfs infomation string
pub const SFS_INFO: &str = "simple file system";
/// max length of infomation
pub const MAX_INFO_LEN: usize = 31;
/// max length of filename
pub const MAX_FNAME_LEN: usize = 255;
/// max file size in theory (48KB + 4MB + 4GB)
/// however, the file size is stored in u32
pub const MAX_FILE_SIZE: usize = 0xffff_ffff;
/// max number of blocks in sfs, use u32 to store the blockId on disk
pub const MAX_NBLOCKS: usize = 0xffff_ffff;
/// block the superblock lives in
pub const BLKN_SUPER: BlockId = 0;
/// location of the root dir inode
pub const BLKN_ROOT: BlockId = 1;
/// 1st block of the freemap
pub const BLKN_FREEMAP: BlockId = 2;
/// number of bits in a block
pub const BLKBITS: usize = BLOCK_SIZE * 8;
/// size of one entry
pub const ENTRY_SIZE: usize = size_of::<u32>();
/// number of entries in a block
pub const BLK_NENTRY: usize = BLOCK_SIZE / ENTRY_SIZE;
/// size of a dirent used in the size field
pub const DIRENT_SIZE: usize = size_of::<DiskEntry>();
/// helper zero block to clean data
pub const ZEROS: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
/// max number of blocks with direct blocks
pub const MAX_NBLOCK_DIRECT: usize = NDIRECT;
/// max number of blocks with indirect blocks
pub const MAX_NBLOCK_INDIRECT: usize = MAX_NBLOCK_DIRECT + BLK_NENTRY;
/// max number of blocks with double indirect blocks
pub const MAX_NBLOCK_DOUBLE_INDIRECT: usize = MAX_NBLOCK_INDIRECT + BLK_NENTRY * BLK_NENTRY;

const_assert!(o1; size_of::<SuperBlock>() <= BLOCK_SIZE);
const_assert!(o2; size_of::<DiskInode>() <= BLOCK_SIZE);
const_assert!(o3; size_of::<DiskEntry>() <= BLOCK_SIZE);
const_assert!(o4; size_of::<IndirectBlock>() == BLOCK_SIZE);
const_assert!(o5; SFS_INFO.len() <= MAX_INFO_LEN);
