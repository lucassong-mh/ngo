/// Present a per-process view of FS.
use super::*;

use super::fspath::FsPathInner;

#[derive(Debug)]
pub struct FsView {
    root: String,
    cwd: RwLock<String>,
}

impl Clone for FsView {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            cwd: RwLock::new(self.cwd()),
        }
    }
}

impl FsView {
    pub fn new() -> FsView {
        let root = String::from("/");
        let cwd = root.clone();
        Self {
            root,
            cwd: RwLock::new(cwd),
        }
    }

    /// Get the root directory
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Get the current working directory.
    pub fn cwd(&self) -> String {
        self.cwd.read().unwrap().clone()
    }

    /// Set the current working directory.
    pub fn set_cwd(&self, path: &str) -> Result<()> {
        if path.len() == 0 {
            return_errno!(EINVAL, "empty path");
        }

        let mut cwd = self.cwd.write().unwrap();
        if let Some('/') = path.chars().next() {
            // absolute
            *cwd = path.to_owned();
        } else {
            // relative
            if !cwd.ends_with("/") {
                *cwd += "/";
            }
            *cwd += path;
        }
        Ok(())
    }

    /// Open a file on the process. But DO NOT add it to file table.
    pub async fn open_file(&self, fs_path: &FsPath, flags: u32, mode: FileMode) -> Result<FileRef> {
        let creation_flags = CreationFlags::from_bits_truncate(flags);
        let inode = if creation_flags.no_follow_symlink() {
            match self.lookup_inode_no_follow(fs_path).await {
                Ok(inode) => {
                    let status_flags = StatusFlags::from_bits_truncate(flags);
                    if inode.metadata().await?.type_ == FileType::SymLink
                        && !status_flags.is_fast_open()
                    {
                        return_errno!(ELOOP, "file is a symlink");
                    }
                    if creation_flags.can_create() && creation_flags.is_exclusive() {
                        return_errno!(EEXIST, "file exists");
                    }
                    if creation_flags.must_be_directory()
                        && inode.metadata().await?.type_ != FileType::Dir
                    {
                        return_errno!(
                            ENOTDIR,
                            "O_DIRECTORY is specified but file is not a directory"
                        );
                    }
                    inode
                }
                Err(e) if e.errno() == ENOENT && creation_flags.can_create() => {
                    let (dir_inode, file_name) = self.lookup_dirinode_and_basename(fs_path).await?;
                    if !dir_inode.allow_write() {
                        return_errno!(EPERM, "file cannot be created");
                    }
                    dir_inode
                        .create(&file_name, FileType::File, mode.bits())
                        .await?
                }
                Err(e) => return Err(e),
            }
        } else {
            match self.lookup_inode(fs_path).await {
                Ok(inode) => {
                    if creation_flags.can_create() && creation_flags.is_exclusive() {
                        return_errno!(EEXIST, "file exists");
                    }
                    if creation_flags.must_be_directory()
                        && inode.metadata().await?.type_ != FileType::Dir
                    {
                        return_errno!(
                            ENOTDIR,
                            "O_DIRECTORY is specified but file is not a directory"
                        );
                    }
                    inode
                }
                Err(e) if e.errno() == ENOENT && creation_flags.can_create() => {
                    // TODO: cannot call async fn recursively?
                    let (dir_inode, file_name) = if !self
                        .convert_fspath_to_abs(fs_path)
                        .await?
                        .trim_start_matches('/')
                        .starts_with(ASYNC_SFS_NAME)
                    {
                        self.lookup_real_dirinode_and_basename_sync(fs_path)?
                    } else {
                        self.lookup_dirinode_and_basename(fs_path).await?
                    };
                    if !dir_inode.allow_write() {
                        return_errno!(EPERM, "file cannot be created");
                    }
                    dir_inode
                        .create(&file_name, FileType::File, mode.bits())
                        .await?
                }
                Err(e) => return Err(e),
            }
        };
        let open_path = self.convert_fspath_to_abs(fs_path).await?;
        if let Some(sync_inode) = inode.as_sync() {
            let inode_file = INodeFile::open(sync_inode, flags, open_path)?;
            Ok(FileRef::new_inode(inode_file))
        } else {
            let async_file_handle = {
                let dentry = Dentry::new(inode.as_async().unwrap(), open_path);
                AsyncFileHandle::open(dentry, flags).await?
            };
            Ok(FileRef::new_async_file_handle(async_file_handle))
        }
    }

    pub fn lookup_inode_sync(&self, fs_path: &FsPath) -> Result<InodeHandle> {
        self.lookup_inode_inner_sync(fs_path, true)
    }

    pub fn lookup_inode_sync_no_follow(&self, fs_path: &FsPath) -> Result<InodeHandle> {
        self.lookup_inode_inner_sync(fs_path, false)
    }

    fn lookup_inode_inner_sync(
        &self,
        fs_path: &FsPath,
        follow_symlink: bool,
    ) -> Result<InodeHandle> {
        let inode = match fs_path.inner() {
            FsPathInner::Absolute(path) | FsPathInner::CwdRelative(path) => {
                if follow_symlink {
                    self.lookup_inode_cwd_sync(path)?
                } else {
                    self.lookup_inode_cwd_sync_no_follow(path)?
                }
            }
            FsPathInner::Cwd => {
                if follow_symlink {
                    self.lookup_inode_cwd_sync(&self.cwd())?
                } else {
                    self.lookup_inode_cwd_sync_no_follow(&self.cwd())?
                }
            }
            FsPathInner::FdRelative(dirfd, path) => {
                let inode = self.lookup_inode_from_fd(*dirfd)?.as_sync().unwrap();
                if follow_symlink {
                    InodeHandle::from_sync(inode.lookup_follow(path, MAX_SYMLINKS)?)
                } else {
                    let (dir_path, base_name) = split_path(path);
                    let dir_inode = inode.lookup_follow(dir_path, MAX_SYMLINKS)?;
                    InodeHandle::from_sync(dir_inode.lookup(base_name)?)
                }
            }
            FsPathInner::Fd(fd) => self.lookup_inode_from_fd(*fd)?,
        };

        Ok(inode)
    }

    fn lookup_inode_cwd_sync_no_follow(&self, path: &str) -> Result<InodeHandle> {
        let (dir_path, file_name) = split_path(&path);
        let dir_inode = self.lookup_inode_cwd_sync(dir_path)?;
        let inode = dir_inode.as_sync().unwrap().lookup(file_name)?;
        Ok(InodeHandle::from_sync(inode))
    }

    /// Lookup INode from the cwd of the process, dereference symlink
    fn lookup_inode_cwd_sync(&self, path: &str) -> Result<InodeHandle> {
        let inode = if let Some('/') = path.chars().next() {
            // absolute path
            let abs_path = path.trim_start_matches('/');
            ROOT_FS
                .read()
                .unwrap()
                .root_inode()
                .lookup_follow(abs_path, MAX_SYMLINKS)?
        } else {
            // relative path
            let cwd = self.cwd();
            ROOT_FS
                .read()
                .unwrap()
                .root_inode()
                .lookup_follow(cwd.trim_start_matches('/'), MAX_SYMLINKS)?
                .lookup_follow(path, MAX_SYMLINKS)?
        };
        Ok(InodeHandle::from_sync(inode))
    }

    /// Lookup Inode, dereference symlink
    pub async fn lookup_inode(&self, fs_path: &FsPath) -> Result<InodeHandle> {
        debug!("lookup_inode: cwd: {:?}, path: {:?}", self.cwd(), fs_path);
        self.lookup_inode_inner(fs_path, true).await
    }

    /// Lookup Inode, do not dereference the last symlink component
    pub async fn lookup_inode_no_follow(&self, fs_path: &FsPath) -> Result<InodeHandle> {
        debug!(
            "lookup_inode_no_follow: cwd: {:?}, path: {:?}",
            self.cwd(),
            fs_path
        );
        self.lookup_inode_inner(fs_path, false).await
    }

    async fn lookup_inode_inner(
        &self,
        fs_path: &FsPath,
        follow_symlink: bool,
    ) -> Result<InodeHandle> {
        let inode = match fs_path.inner() {
            FsPathInner::Absolute(path) | FsPathInner::CwdRelative(path) => {
                if follow_symlink {
                    self.lookup_inode_cwd(path).await?
                } else {
                    self.lookup_inode_cwd_no_follow(path).await?
                }
            }
            FsPathInner::Cwd => {
                if follow_symlink {
                    self.lookup_inode_cwd(&self.cwd()).await?
                } else {
                    self.lookup_inode_cwd_no_follow(&self.cwd()).await?
                }
            }
            FsPathInner::FdRelative(dirfd, path) => {
                let inode = self.lookup_inode_from_fd(*dirfd)?;
                if follow_symlink {
                    inode.lookup(path, MAX_SYMLINKS).await?
                } else {
                    let (dir_path, base_name) = split_path(path);
                    let dir_inode = inode.lookup(dir_path, MAX_SYMLINKS).await?;
                    dir_inode.lookup_no_follow(base_name).await?
                }
            }
            FsPathInner::Fd(fd) => self.lookup_inode_from_fd(*fd)?,
        };

        Ok(inode)
    }

    /// Lookup dir inode and basename
    pub async fn lookup_dirinode_and_basename(
        &self,
        fs_path: &FsPath,
    ) -> Result<(InodeHandle, String)> {
        let (dir_inode, base_name) = match fs_path.inner() {
            FsPathInner::Absolute(path) | FsPathInner::CwdRelative(path) => {
                let (dir_path, base_name) = split_path(path);
                (self.lookup_inode_cwd(dir_path).await?, base_name.to_owned())
            }
            FsPathInner::FdRelative(dirfd, path) => {
                let inode = self.lookup_inode_from_fd(*dirfd)?;
                let (dir_path, base_name) = split_path(path);
                let dir_inode = inode.lookup(dir_path, MAX_SYMLINKS).await?;
                (dir_inode, base_name.to_owned())
            }
            _ => return_errno!(ENOENT, "cannot find dir and basename with empty path"),
        };
        Ok((dir_inode, base_name))
    }

    /// Lookup the real dir inode and basename.
    /// It is used to create new file in `open_file`.
    fn lookup_real_dirinode_and_basename_sync(
        &self,
        fs_path: &FsPath,
    ) -> Result<(InodeHandle, String)> {
        let (dir_inode, base_name) = match fs_path.inner() {
            FsPathInner::Absolute(path) | FsPathInner::CwdRelative(path) => {
                let real_path = self.lookup_real_path_sync(None, path)?;
                let (dir_path, base_name) = split_path(&real_path);
                (self.lookup_inode_cwd_sync(dir_path)?, base_name.to_owned())
            }
            FsPathInner::FdRelative(dirfd, path) => {
                let inode = self.lookup_inode_from_fd(*dirfd)?;
                let real_path = self.lookup_real_path_sync(Some(&inode), path)?;
                let (dir_path, base_name) = split_path(&real_path);
                let dir_inode = if let Some('/') = dir_path.chars().next() {
                    self.lookup_inode_cwd_sync(dir_path)?
                } else {
                    let dir_inode = inode
                        .as_sync()
                        .unwrap()
                        .lookup_follow(dir_path, MAX_SYMLINKS)?;
                    InodeHandle::from_sync(dir_inode)
                };
                (dir_inode, base_name.to_owned())
            }
            _ => return_errno!(ENOENT, "cannot find real dir and basename with empty path"),
        };
        Ok((dir_inode, base_name))
    }

    /// Lookup Inode from the cwd of the process. If path is a symlink, do not dereference it
    async fn lookup_inode_cwd_no_follow(&self, path: &str) -> Result<InodeHandle> {
        let (dir_path, file_name) = split_path(&path);
        let dir_inode = self.lookup_inode_cwd(dir_path).await?;
        Ok(dir_inode.lookup_no_follow(file_name).await?)
    }

    /// Lookup Inode from the cwd of the process, dereference symlink
    async fn lookup_inode_cwd(&self, path: &str) -> Result<InodeHandle> {
        if let Some('/') = path.chars().next() {
            // absolute path
            let abs_path = path.trim_start_matches('/');
            let inode = if abs_path.starts_with(ASYNC_SFS_NAME) {
                let path = abs_path.strip_prefix(ASYNC_SFS_NAME).unwrap();
                let inode = ASYNC_SFS
                    .get()
                    .await
                    .root_inode()
                    .await
                    .lookup_follow(path, MAX_SYMLINKS)
                    .await?;
                InodeHandle::from_async(inode)
            } else {
                let inode = ROOT_FS
                    .read()
                    .unwrap()
                    .root_inode()
                    .lookup_follow(abs_path, MAX_SYMLINKS)?;
                InodeHandle::from_sync(inode)
            };
            Ok(inode)
        } else {
            // relative path
            let cwd_string = self.cwd();
            let cwd = cwd_string.trim_start_matches('/');
            let inode = if cwd.starts_with(ASYNC_SFS_NAME) {
                let cwd = cwd.strip_prefix(ASYNC_SFS_NAME).unwrap();
                let inode = ASYNC_SFS
                    .get()
                    .await
                    .root_inode()
                    .await
                    .lookup_follow(cwd, MAX_SYMLINKS)
                    .await?
                    .lookup_follow(path, MAX_SYMLINKS)
                    .await?;
                InodeHandle::from_async(inode)
            } else if cwd == "/" && path.starts_with(ASYNC_SFS_NAME) {
                let path = path.strip_prefix(ASYNC_SFS_NAME).unwrap();
                let inode = ASYNC_SFS
                    .get()
                    .await
                    .root_inode()
                    .await
                    .lookup_follow(path, MAX_SYMLINKS)
                    .await?;
                InodeHandle::from_async(inode)
            } else {
                let inode = ROOT_FS
                    .read()
                    .unwrap()
                    .root_inode()
                    .lookup_follow(cwd, MAX_SYMLINKS)?
                    .lookup_follow(path, MAX_SYMLINKS)?;
                InodeHandle::from_sync(inode)
            };
            Ok(inode)
        }
    }

    fn lookup_inode_from_fd(&self, fd: FileDesc) -> Result<InodeHandle> {
        let file_ref = current!().file(fd)?;
        let inode = if let Some(inode_file) = file_ref.as_inode_file() {
            let inode = Arc::clone(inode_file.inode());
            InodeHandle::from_sync(inode)
        } else if let Some(async_file_handle) = file_ref.as_async_file_handle() {
            let inode = Arc::clone(async_file_handle.dentry().inode());
            InodeHandle::from_async(inode)
        } else {
            return_errno!(EBADF, "dirfd is not an inode file");
        };
        Ok(inode)
    }

    /// Recursively lookup the real path of giving path, dereference symlinks.
    /// If parent is provided, it will lookup the real path from the parent inode.
    /// If parent is not provided, it will lookup the real path from the cwd of process.
    fn lookup_real_path_sync(&self, parent: Option<&InodeHandle>, path: &str) -> Result<String> {
        let (dir_path, file_name) = split_path(&path);
        let dir_inode = if let Some(parent_inode) = parent {
            if let Some('/') = path.chars().next() {
                self.lookup_inode_cwd_sync(dir_path)?
            } else {
                // relative path from parent inode
                let dir_inode = parent_inode
                    .as_sync()
                    .unwrap()
                    .lookup_follow(dir_path, MAX_SYMLINKS)?;
                InodeHandle::from_sync(dir_inode)
            }
        } else {
            self.lookup_inode_cwd_sync(dir_path)?
        };

        match dir_inode.as_sync().unwrap().lookup(file_name) {
            // Handle symlink
            Ok(inode) if inode.metadata()?.type_ == FileType::SymLink => {
                let new_path = {
                    let path = {
                        let mut content = vec![0u8; PATH_MAX];
                        let len = inode.read_at(0, &mut content)?;
                        let path = std::str::from_utf8(&content[..len])
                            .map_err(|_| errno!(ENOENT, "invalid symlink content"))?;
                        String::from(path)
                    };
                    match path.chars().next() {
                        // absolute path
                        Some('/') => path,
                        // relative path
                        Some(_) => {
                            let dir_path = if dir_path.ends_with("/") {
                                String::from(dir_path)
                            } else {
                                String::from(dir_path) + "/"
                            };
                            dir_path + &path
                        }
                        None => unreachable!(),
                    }
                };
                self.lookup_real_path_sync(parent, &new_path)
            }
            Err(FsError::EntryNotFound) | Ok(_) => {
                debug!("real_path: {:?}", path);
                Ok(String::from(path))
            }
            Err(e) => return Err(Error::from(e)),
        }
    }

    /// Convert the FsPath to the absolute path.
    /// This function is used to record the open path for a file.
    ///
    /// TODO: Introducing dentry cache to get the full path from inode.
    pub async fn convert_fspath_to_abs(&self, fs_path: &FsPath) -> Result<String> {
        let abs_path = match fs_path.inner() {
            FsPathInner::Absolute(path) => (*path).to_owned(),
            FsPathInner::CwdRelative(path) => {
                if !self.cwd().ends_with("/") {
                    self.cwd().to_owned() + "/" + path
                } else {
                    self.cwd().to_owned() + path
                }
            }
            FsPathInner::FdRelative(dirfd, path) => {
                let file_ref = current!().file(*dirfd)?;

                let dir_path = if let Some(inode_file) = file_ref.as_inode_file() {
                    if inode_file.inode().metadata()?.type_ != FileType::Dir {
                        return_errno!(ENOTDIR, "dirfd is not a directory");
                    }
                    inode_file.open_path().to_owned()
                } else if let Some(async_file_handle) = file_ref.as_async_file_handle() {
                    if async_file_handle.dentry().inode().metadata().await?.type_ != FileType::Dir {
                        return_errno!(ENOTDIR, "dirfd is not a directory");
                    }
                    async_file_handle.dentry().open_path().to_owned()
                } else {
                    return_errno!(EBADF, "dirfd is not an inode file");
                };
                if !dir_path.ends_with("/") {
                    dir_path + "/" + path
                } else {
                    dir_path + path
                }
            }
            FsPathInner::Cwd => self.cwd().to_owned(),
            FsPathInner::Fd(fd) => {
                let file_ref = current!().file(*fd)?;
                if let Some(inode_file) = file_ref.as_inode_file() {
                    inode_file.open_path().to_owned()
                } else if let Some(async_file_handle) = file_ref.as_async_file_handle() {
                    async_file_handle.dentry().open_path().to_owned()
                } else {
                    return_errno!(EBADF, "dirfd is not an inode file");
                }
            }
        };
        Ok(abs_path)
    }
}

impl Default for FsView {
    fn default() -> Self {
        Self::new()
    }
}

// Linux uses 40 as the upper limit for resolving symbolic links,
// so Occlum use it as a reasonable value
pub const MAX_SYMLINKS: usize = 40;
