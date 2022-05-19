use super::*;

use async_once::AsyncOnce;
use async_sfs::AsyncSimpleFS;
use block_device::{BlockDevice, BLOCK_SIZE};
use sgx_disk::{HostDisk, IoUringDisk, SyncIoDisk};
use std::path::{Path, PathBuf};
use std::untrusted::path::PathEx;

pub const ASYNC_SFS_NAME: &str = "async_sfs";
const ASYNC_SFS_IMAGE_PATH: &str = "./run/async_sfs.image";

lazy_static! {
    /// Async sfs
    pub static ref ASYNC_SFS: AsyncOnce<Arc<dyn AsyncFileSystem>> = AsyncOnce::new(async{
        let image_path = PathBuf::from(ASYNC_SFS_IMAGE_PATH);
        let async_sfs = if image_path.exists() {
            //let iouring_disk = IoUringDisk::<runtime::IoUringRuntime>::open(&image_path).unwrap();
            let sync_disk = SyncIoDisk::open(&image_path).unwrap();
            AsyncSimpleFS::open(Arc::new(sync_disk)).await.unwrap()
        } else {
            const GB: usize = 1024 * 1024 * 1024;
            let total_blocks = 8 * GB / BLOCK_SIZE;
            //let iouring_disk = IoUringDisk::<runtime::IoUringRuntime>::create_new(&image_path, total_blocks).unwrap();
            let sync_disk = SyncIoDisk::create_new(&image_path, total_blocks).unwrap();
            AsyncSimpleFS::create(Arc::new(sync_disk)).await.unwrap()
        };
        async_sfs as _
    });
}

mod runtime {
    use io_uring_callback::IoUring;
    use sgx_disk::IoUringProvider;

    pub struct IoUringRuntime;

    impl IoUringProvider for IoUringRuntime {
        fn io_uring() -> &'static IoUring {
            &*crate::io_uring::SINGLETON
        }
    }
}
