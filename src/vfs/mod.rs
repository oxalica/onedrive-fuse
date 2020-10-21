use crate::login::ManagedOnedrive;
use onedrive_api::OneDrive;
use serde::Deserialize;
use std::{ffi::OsStr, ops::Deref, time::Duration};

mod dir;
pub mod error;
mod inode;
mod statfs;

pub use dir::DirEntry;
pub use error::{Error, Result};
pub use inode::InodeAttr;
pub use statfs::StatfsData;

#[derive(Debug, Deserialize)]
pub struct Config {
    statfs: statfs::Config,
    inode: inode::Config,
    dir: dir::Config,
}

pub struct Vfs {
    statfs: statfs::Statfs,
    inode_pool: inode::InodePool,
    dir_pool: dir::DirPool,
    onedrive: ManagedOnedrive,
}

impl Vfs {
    pub async fn new(config: Config, onedrive: ManagedOnedrive) -> Result<Self> {
        let inode_pool = inode::InodePool::new(config.inode, &*onedrive.get().await).await?;
        Ok(Self {
            statfs: statfs::Statfs::new(config.statfs),
            inode_pool,
            dir_pool: dir::DirPool::new(config.dir),
            onedrive,
        })
    }

    async fn onedrive(&self) -> impl Deref<Target = OneDrive> + '_ {
        self.onedrive.get().await
    }

    pub async fn statfs(&self) -> Result<(StatfsData, Duration)> {
        self.statfs.statfs(&*self.onedrive().await).await
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
    ) -> Result<(u64, InodeAttr, Duration)> {
        self.inode_pool
            .lookup(
                parent_ino,
                child_name,
                &self.dir_pool,
                &*self.onedrive().await,
            )
            .await
    }

    pub async fn forget(&self, ino: u64, count: u64) -> Result<()> {
        self.inode_pool.free(ino, count).await
    }

    pub async fn get_attr(&self, ino: u64) -> Result<(InodeAttr, Duration)> {
        self.inode_pool.get_attr(ino, &*self.onedrive().await).await
    }

    pub async fn open_dir(&self, ino: u64) -> Result<u64> {
        let item_id = self.inode_pool.get_item_id(ino)?;
        let fh = self
            .dir_pool
            .open(ino, item_id, &self.inode_pool, &*self.onedrive().await)
            .await?;
        Ok(fh)
    }

    pub async fn close_dir(&self, _ino: u64, fh: u64) -> Result<()> {
        self.dir_pool.free(fh)
    }

    pub async fn read_dir(
        &self,
        _ino: u64,
        fh: u64,
        offset: u64,
    ) -> Result<impl AsRef<[DirEntry]>> {
        self.dir_pool.read(fh, offset).await
    }
}
