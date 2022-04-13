use super::*;

pub async fn do_sync() -> Result<()> {
    debug!("sync:");
    ROOT_FS.read().unwrap().sync()?;
    ASYNC_SFS.get().await.sync().await?;
    Ok(())
}
