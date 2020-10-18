use crate::error::Result;
use onedrive_api::{ItemLocation, OneDrive};
use serde::Deserialize;
use std::{ffi::OsStr, time::Duration};

mod dir;
mod inode;
mod statfs;
pub use dir::DirEntry;
pub use inode::InodeAttr;
pub use statfs::StatfsData;

#[derive(Deserialize)]
pub struct Config {
    statfs: statfs::Config,
    inode: inode::Config,
    dir: dir::Config,
}

pub struct Vfs {
    statfs: statfs::Statfs,
    inode_pool: inode::InodePool,
    dir_pool: dir::DirPool,
}

impl Vfs {
    pub async fn new(config: Config, onedrive: &OneDrive) -> Result<Self> {
        use onedrive_api::{option::ObjectOption, resource::DriveItemField};
        let root_item_id = onedrive
            .get_item_with_option(
                ItemLocation::root(),
                ObjectOption::new().select(&[DriveItemField::id]),
            )
            .await?
            .expect("No If-None-Match")
            .id
            .expect("`id` is selected");

        Ok(Self {
            statfs: statfs::Statfs::new(config.statfs),
            inode_pool: inode::InodePool::new(root_item_id, config.inode).await,
            dir_pool: dir::DirPool::new(config.dir),
        })
    }

    pub async fn statfs(&self, onedrive: &OneDrive) -> Result<(StatfsData, Duration)> {
        self.statfs.statfs(onedrive).await
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
        onedrive: &OneDrive,
    ) -> Result<(u64, InodeAttr, Duration)> {
        self.inode_pool
            .lookup(parent_ino, child_name, onedrive)
            .await
    }

    pub async fn forget(&self, ino: u64, count: u64) -> Result<()> {
        self.inode_pool.free(ino, count).await
    }

    pub async fn get_attr(&self, ino: u64, onedrive: &OneDrive) -> Result<(InodeAttr, Duration)> {
        self.inode_pool.get_attr(ino, onedrive).await
    }

    pub async fn open_dir(&self, ino: u64) -> Result<u64> {
        let item_id = self.inode_pool.get_item_id(ino)?;
        let cache = self.inode_pool.get_dir_cache(ino).expect("Already checked");
        let fh = self.dir_pool.open(item_id, &cache);
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
        onedrive: &OneDrive,
    ) -> Result<impl AsRef<[DirEntry]>> {
        self.dir_pool
            .read(fh, offset, &self.inode_pool, onedrive)
            .await
    }
}
