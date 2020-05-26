use crate::{
    error::{Error, Result},
    util::de_duration_sec,
    vfs::dir,
};
use onedrive_api::{FileName, ItemId, ItemLocation, OneDrive};
use serde::Deserialize;
use sharded_slab::{Clear, Pool};
use std::{
    collections::hash_map::{Entry, HashMap},
    convert::TryFrom as _,
    ffi::OsStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use time::Timespec;
use tokio::sync::Mutex;

#[derive(Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "de_duration_sec")]
    attr_cache_ttl: Duration,
}

// This should not hold any heap-allocation due to the requirement `Inode: Clear`.
#[derive(Clone, Copy)]
pub struct InodeAttr {
    pub size: u64,
    pub mtime: Timespec,
    pub crtime: Timespec,
    pub is_directory: bool,
}

pub struct InodePool {
    /// ino_shift = ino - key
    ino_shift: u64,
    pool: Pool<Inode>,
    rev_map: Mutex<HashMap<ItemId, usize>>,
    config: Config,
}

struct Inode {
    ref_count: AtomicU64,
    item_id: ItemId,
    attr_cache: Arc<Mutex<Option<(InodeAttr, Instant)>>>,
    // FIXME: Remove this.
    dir_cache: dir::Cache,
}

impl Clear for Inode {
    fn clear(&mut self) {
        // FIXME
        *self = Default::default();
    }
}

// Required by `Pool`. Set to an invalid state.
impl Default for Inode {
    fn default() -> Self {
        Self {
            ref_count: 0.into(),
            item_id: ItemId(String::new()),
            attr_cache: Default::default(),
            dir_cache: Default::default(),
        }
    }
}

impl Inode {
    // TODO: Initialize InodeAttrs.
    fn new(item_id: ItemId) -> Self {
        Self {
            ref_count: 1.into(),
            item_id,
            attr_cache: Default::default(),
            dir_cache: Default::default(),
        }
    }
}

const ROOT_INO: u64 = fuse::FUSE_ROOT_ID;
static_assertions::const_assert_eq!(ROOT_INO, 1);

impl InodePool {
    /// Initialize inode pool with root id to make operation on root nothing special.
    pub async fn new(root_item_id: ItemId, config: Config) -> Self {
        let mut ret = Self {
            ino_shift: 0,
            pool: Default::default(),
            rev_map: Default::default(),
            config,
        };
        // Root has ref-count initialized at 1.
        let root_key = ret.get_or_alloc(root_item_id).await;
        ret.ino_shift = ROOT_INO - u64::try_from(root_key).unwrap();
        ret
    }

    fn key_to_ino(&self, key: usize) -> u64 {
        u64::try_from(key).unwrap().wrapping_add(self.ino_shift)
    }

    fn ino_to_key(&self, ino: u64) -> usize {
        usize::try_from(ino.wrapping_sub(self.ino_shift)).unwrap()
    }

    async fn get_or_alloc(&self, item_id: ItemId) -> usize {
        match self.rev_map.lock().await.entry(item_id) {
            Entry::Occupied(ent) => {
                let key = *ent.get();
                self.pool
                    .get(key)
                    .unwrap()
                    .ref_count
                    .fetch_add(1, Ordering::Relaxed);
                key
            }
            Entry::Vacant(ent) => {
                let key = self
                    .pool
                    .create(|p| *p = Inode::new(ent.key().clone()))
                    .expect("Pool is full");
                ent.insert(key);
                key
            }
        }
    }

    async fn free(&self, key: usize, count: u64) -> Option<()> {
        // Lock first to avoid race with get_or_alloc.
        let mut rev_g = self.rev_map.lock().await;
        let g = self.pool.get(key)?;
        let orig_ref_count = g.ref_count.fetch_sub(count, Ordering::Relaxed);
        if count < orig_ref_count {
            return Some(());
        }
        assert!(rev_g.remove(&g.item_id).is_some());
        drop(g);
        assert!(self.pool.clear(key));
        Some(())
    }

    /// Get inode by item_id without increasing its ref-count.
    /// Inode data may or may not be cached.
    ///
    /// This is used in `readdir`.
    // TODO: Cache ItemId and InodeAttr.
    pub async fn touch(&self, item_id: ItemId) -> u64 {
        let key = self.get_or_alloc(item_id).await;
        self.free(key, 1).await.unwrap();
        self.key_to_ino(key)
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
        let parent_item_id = self.get_item_id(parent_ino).expect("Invalid inode");
        let child_name = cvt_filename(child_name)?;
        let (item_id, attr) = Self::get_attr_raw(
            ItemLocation::child_of_id(&parent_item_id, child_name),
            onedrive,
        )
        .await?;

        let key = self.get_or_alloc(item_id).await;

        // Fresh cache.
        let cache = self.pool.get(key).unwrap().attr_cache.clone();
        *cache.lock().await = Some((attr, Instant::now()));

        let ino = self.key_to_ino(key);
        let ttl = self.config.attr_cache_ttl;
        Ok((ino, attr, ttl))
    }

    pub async fn forget(&self, ino: u64, count: u64) -> Option<()> {
        self.free(self.ino_to_key(ino), count).await
    }

    pub async fn get_attr(&self, ino: u64, onedrive: &OneDrive) -> Result<(InodeAttr, Duration)> {
        let key = self.ino_to_key(ino);

        // Check from cache.
        let cache = self
            .pool
            .get(key)
            .expect("Invalid inode")
            .attr_cache
            .clone();
        let mut cache = cache.lock().await;
        if let Some((last_attr, last_inst)) = &*cache {
            if let Some(ttl) = self.config.attr_cache_ttl.checked_sub(last_inst.elapsed()) {
                return Ok((*last_attr, ttl));
            }
        }

        // Cache miss. Hold the mutex during the request.
        log::debug!("get_attr: cache miss");
        let item_id = self.pool.get(key).expect("Invalid inode").item_id.clone();
        let (_, attr) = Self::get_attr_raw(ItemLocation::from_id(&item_id), onedrive).await?;
        // Fresh cache.
        *cache = Some((attr, Instant::now()));

        let ttl = self.config.attr_cache_ttl;
        Ok((attr, ttl))
    }

    // FIXME: Only used by DirPool.
    pub fn get_item_id(&self, ino: u64) -> Option<ItemId> {
        Some(self.pool.get(self.ino_to_key(ino))?.item_id.clone())
    }

    // FIXME: Remove this.
    pub fn get_dir_cache(&self, ino: u64) -> Option<dir::Cache> {
        Some(self.pool.get(self.ino_to_key(ino))?.dir_cache.clone())
    }
}

fn cvt_filename<'a>(name: &'a OsStr) -> Result<&'a FileName> {
    name.to_str()
        .and_then(FileName::new)
        .ok_or(Error::InvalidArgument("Invalid filename"))
}
