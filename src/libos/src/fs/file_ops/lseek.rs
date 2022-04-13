use super::*;

pub fn do_lseek(fd: FileDesc, offset: SeekFrom) -> Result<usize> {
    debug!("lseek: fd: {:?}, offset: {:?}", fd, offset);
    let file_ref = current!().file(fd)?;
    if let Some(inode_file) = file_ref.as_inode_file() {
        inode_file.seek(offset)
    } else if let Some(disk_file) = file_ref.as_disk_file() {
        disk_file.seek(offset)
    } else if let Some(async_file_handle) = file_ref.as_async_file_handle() {
        async_file_handle.seek(offset)
    } else {
        return_errno!(EBADF, "not an inode");
    }
}
