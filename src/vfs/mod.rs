use crate::{
    error::{Error, Result},
    util::de_duration_sec,
};
use fuse::FUSE_ROOT_ID;
use onedrive_api::{FileName, ItemId, ItemLocation, OneDrive};
use serde::Deserialize;
use sharded_slab::Clear;
use std::{
    ffi::OsStr,
    sync::Arc,
    time::{Duration, Instant},
};
use time::Timespec;
use tokio::sync::Mutex;

mod dir;
mod inode;
mod statfs;
pub use dir::DirEntry;
pub use inode::InodeAttr;
pub use statfs::StatfsData;

#[derive(Deserialize)]
pub struct Config {
    statfs: statfs::Config,
    inode: InodeConfig,
}

#[derive(Deserialize)]
pub struct InodeConfig {
    #[serde(deserialize_with = "de_duration_sec")]
    attr_cache_ttl: Duration,
}

pub struct Vfs {
    inode_config: InodeConfig,
    statfs: statfs::Statfs,
    inode_pool: inode::InodePool<InodeData>,
    dir_pool: dir::DirPool,
}

#[derive(Default)]
struct InodeData {
    /// FIXME: Remove `Arc`. https://github.com/hawkw/sharded-slab/issues/43
    attr_cache: Arc<Mutex<Option<(InodeAttr, Instant)>>>,
}

impl Clear for InodeData {
    fn clear(&mut self) {
        // Avoid pollution.
        self.attr_cache = Default::default();
    }
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
            inode_pool: inode::InodePool::new(root_item_id).await,
            dir_pool: Default::default(),
            inode_config: config.inode,
        })
    }

    pub async fn statfs(&self, onedrive: &OneDrive) -> Result<(StatfsData, Duration)> {
        self.statfs.statfs(onedrive).await
    }

    async fn get_attr_raw(
        loc: ItemLocation<'_>,
        onedrive: &OneDrive,
    ) -> Result<(ItemId, InodeAttr)> {
        use onedrive_api::{option::ObjectOption, resource::DriveItemField};

        fn parse_time(s: &str) -> Timespec {
            // FIXME
            time::strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z")
                .or_else(|_| time::strptime(s, "%Y-%m-%dT%H:%M:%S%z"))
                .unwrap_or_else(|err| panic!("Invalid time '{}': {}", s, err))
                .to_timespec()
        }

        // TODO: If-None-Match
        let item = onedrive
            .get_item_with_option(
                loc,
                ObjectOption::new().select(&[
                    DriveItemField::id,
                    DriveItemField::size,
                    DriveItemField::last_modified_date_time,
                    DriveItemField::created_date_time,
                    DriveItemField::folder,
                ]),
            )
            .await?
            .expect("No If-None-Match");

        let item_id = item.id.unwrap();
        let attr = InodeAttr {
            size: item.size.unwrap() as u64,
            mtime: parse_time(item.last_modified_date_time.as_deref().unwrap()),
            crtime: parse_time(item.created_date_time.as_deref().unwrap()),
            is_directory: item.folder.is_some(),
        };
        Ok((item_id, attr))
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
        onedrive: &OneDrive,
    ) -> Result<(u64, InodeAttr, Duration)> {
        // Check from directory cache first.
        let parent_item_id = self
            .inode_pool
            .get_item_id(parent_ino)
            .expect("Invalid inode");
        let child_name = cvt_filename(child_name)?;
        let (item_id, attr) = Self::get_attr_raw(
            ItemLocation::child_of_id(&parent_item_id, child_name),
            onedrive,
        )
        .await?;

        let ino = self.inode_pool.get_or_alloc_ino(item_id).await;

        // Fresh cache.
        let cache = self.inode_pool.get_data(ino).unwrap().attr_cache.clone();
        *cache.lock().await = Some((attr, Instant::now()));

        let ttl = self.inode_config.attr_cache_ttl;
        Ok((ino, attr, ttl))
    }

    pub async fn forget(&self, ino: u64, count: u64) {
        self.inode_pool
            .free(ino, count)
            .await
            .expect("Invalid inode");
    }

    pub async fn get_attr(&self, ino: u64, onedrive: &OneDrive) -> Result<(InodeAttr, Duration)> {
        // Check from cache.
        let cache = self
            .inode_pool
            .get_data(ino)
            .expect("Invalid inode")
            .attr_cache
            .clone();
        let mut cache = cache.lock().await;
        if let Some((last_attr, last_inst)) = &*cache {
            if let Some(ttl) = self
                .inode_config
                .attr_cache_ttl
                .checked_sub(last_inst.elapsed())
            {
                return Ok((*last_attr, ttl));
            }
        }

        // Cache miss. Hold the mutex during the request.
        log::debug!("get_attr: cache miss");
        let item_id = self.inode_pool.get_item_id(ino).expect("Invalid inode");
        let (_, attr) = Self::get_attr_raw(ItemLocation::from_id(&item_id), onedrive).await?;
        // Fresh cache.
        *cache = Some((attr, Instant::now()));

        let ttl = self.inode_config.attr_cache_ttl;
        Ok((attr, ttl))
    }

    pub async fn open_dir(&self, ino: u64) -> Result<u64> {
        if ino == FUSE_ROOT_ID {
            Ok(self.dir_pool.alloc(None).await)
        } else {
            let item_id = self.inode_pool.get_item_id(ino).expect("Invalid inode");
            Ok(self.dir_pool.alloc(Some(item_id)).await)
        }
    }

    pub async fn close_dir(&self, _ino: u64, fh: u64) {
        self.dir_pool.free(fh).await.expect("Invalid fh");
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

fn cvt_filename<'a>(name: &'a OsStr) -> Result<&'a FileName> {
    name.to_str()
        .and_then(FileName::new)
        .ok_or(Error::InvalidArgument("Invalid filename"))
}
