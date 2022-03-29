use crate::prelude::*;

use crate::inode::AsyncInode;

/// The Dentry is used to speed up the pathname lookup
pub struct Dentry {
    inode: Arc<dyn AsyncInode>,
    open_path: String,
}

impl Dentry {
    pub fn new(inode: Arc<dyn AsyncInode>, open_path: String) -> Self {
        Self { inode, open_path }
    }

    pub fn inode(&self) -> &Arc<dyn AsyncInode> {
        &self.inode
    }

    // TODO: lookup parent dentry to get the path
    pub fn open_path(&self) -> &str {
        &self.open_path
    }
}
