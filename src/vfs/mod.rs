use crate::login::ManagedOnedrive;
use onedrive_api::OneDrive;
use reqwest::Client;
use serde::Deserialize;
use std::{ffi::OsStr, ops::Deref, time::Duration};

mod dir;
pub mod error;
mod file;
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
    file: file::Config,
}

pub struct Vfs {
    statfs: statfs::Statfs,
    inode_pool: inode::InodePool,
    dir_pool: dir::DirPool,
    file_pool: file::FilePool,
    onedrive: ManagedOnedrive,
    client: Client,
}

impl Vfs {
    pub async fn new(config: Config, onedrive: ManagedOnedrive) -> Result<Self> {
        let inode_pool = inode::InodePool::new(config.inode, &*onedrive.get().await).await?;
        Ok(Self {
            statfs: statfs::Statfs::new(config.statfs),
            inode_pool,
            dir_pool: dir::DirPool::new(config.dir),
            file_pool: file::FilePool::new(config.file),
            onedrive,
            client: Client::new(),
        })
    }

    async fn onedrive(&self) -> impl Deref<Target = OneDrive> + '_ {
        self.onedrive.get().await
    }

    pub async fn statfs(&self) -> Result<(StatfsData, Duration)> {
        let (ret, ttl) = self.statfs.statfs(&*self.onedrive().await).await?;
        log::trace!(target: "vfs::statfs", "statfs: statfs={:?} ttl={:?}", ret, ttl);
        Ok((ret, ttl))
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
    ) -> Result<(u64, InodeAttr, Duration)> {
        let (ino, attr, ttl) = self
            .inode_pool
            .lookup(
                parent_ino,
                child_name,
                &self.dir_pool,
                &*self.onedrive().await,
            )
            .await?;
        log::trace!(target: "vfs::inode", "lookup: ino={} attr={:?} ttl={:?}", ino, attr, ttl);
        Ok((ino, attr, ttl))
    }

    pub async fn forget(&self, ino: u64, count: u64) -> Result<()> {
        let freed = self.inode_pool.free(ino, count).await?;
        log::trace!(target: "vfs::inode", "forget: ino={} count={} freed={}", ino, count, freed);
        Ok(())
    }

    pub async fn get_attr(&self, ino: u64) -> Result<(InodeAttr, Duration)> {
        let (attr, ttl) = self
            .inode_pool
            .get_attr(ino, &*self.onedrive().await)
            .await?;
        log::trace!(target: "vfs::inode", "get_attr: ino={} attr={:?} ttl={:?}", ino, attr, ttl);
        Ok((attr, ttl))
    }

    pub async fn open_dir(&self, ino: u64) -> Result<u64> {
        let item_id = self.inode_pool.get_item_id(ino)?;
        let fh = self
            .dir_pool
            .open(&item_id, &self.inode_pool, &*self.onedrive().await)
            .await?;
        log::trace!(target: "vfs::dir", "open_dir: ino={} fh={}", ino, fh);
        Ok(fh)
    }

    pub async fn close_dir(&self, ino: u64, fh: u64) -> Result<()> {
        self.dir_pool.close(fh)?;
        log::trace!(target: "vfs::dir", "close_dir: ino={} fh={}", ino, fh);
        Ok(())
    }

    pub async fn read_dir(&self, ino: u64, fh: u64, offset: u64) -> Result<impl AsRef<[DirEntry]>> {
        let ret = self.dir_pool.read(fh, offset).await?;
        log::trace!(target: "vfs::dir", "read_dir: ino={} fh={} offset={}", ino, fh, offset);
        Ok(ret)
    }

    // TODO: Flags.
    pub async fn open_file(&self, ino: u64) -> Result<u64> {
        let item_id = self.inode_pool.get_item_id(ino)?;
        let fh = self
            .file_pool
            .open(&item_id, &*self.onedrive().await)
            .await?;
        log::trace!(target: "vfs::file", "open_file: ino={} fh={}", ino, fh);
        Ok(fh)
    }

    pub async fn close_file(&self, ino: u64, fh: u64) -> Result<()> {
        self.file_pool.close(fh).await?;
        log::trace!(target: "vfs::file", "close_file: ino={} fh={}", ino, fh);
        Ok(())
    }

    pub async fn read_file(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        size: usize,
    ) -> Result<impl AsRef<[u8]>> {
        let ret = self.file_pool.read(fh, offset, size, &self.client).await?;
        log::trace!(
            target: "vfs::file",
            "read_file: ino={} fh={} offset={} size={} bytes_read={}",
            ino,
            fh,
            offset,
            size,
            ret.as_ref().len(),
        );
        Ok(ret)
    }
}
