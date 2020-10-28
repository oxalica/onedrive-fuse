use crate::{
    config::de_duration_sec,
    vfs::{
        error::{Error, Result},
        inode,
    },
};
use lru_cache::LruCache;
use onedrive_api::{
    option::ObjectOption, resource::DriveItemField, ItemId, ItemLocation, OneDrive, Tag,
};
use serde::Deserialize;
use sharded_slab::Slab;
use std::{
    collections::HashMap,
    convert::TryFrom,
    ffi::OsString,
    sync::{Arc, Mutex as SyncMutex},
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct DirEntry {
    pub item_id: ItemId,
    pub name: OsString,
    pub attr: inode::InodeAttr,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    lru_cache_size: usize,
    #[serde(deserialize_with = "de_duration_sec")]
    cache_ttl: Duration,
}

pub struct DirPool {
    opened_handles: Slab<Arc<DirSnapshot>>,
    /// ItemId -> (DirSnapshot, fetch_time)
    lru_cache: SyncMutex<LruCache<ItemId, (Arc<DirSnapshot>, Instant)>>,
    config: Config,
}

struct DirSnapshot {
    c_tag: Tag,
    entries: Vec<DirEntry>,
    /// name -> index of `entries`
    name_map: HashMap<String, usize>,
}

impl DirPool {
    pub fn new(config: Config) -> Self {
        Self {
            opened_handles: Slab::new(),
            lru_cache: SyncMutex::new(LruCache::new(config.lru_cache_size)),
            config,
        }
    }

    fn key_to_fh(key: usize) -> u64 {
        u64::try_from(key).unwrap()
    }

    fn fh_to_key(fh: u64) -> usize {
        usize::try_from(fh).unwrap()
    }

    fn alloc(&self, snapshot: Arc<DirSnapshot>) -> usize {
        self.opened_handles.insert(snapshot).expect("Pool is full")
    }

    pub async fn open(
        &self,
        item_id: &ItemId,
        inode_pool: &inode::InodePool,
        onedrive: &OneDrive,
    ) -> Result<u64> {
        // Check directory content cache of the given inode.
        let prev_snapshot = match self.lru_cache.lock().unwrap().get_mut(item_id).cloned() {
            // Cache hit.
            Some((snapshot, last_checked)) if last_checked.elapsed() < self.config.cache_ttl => {
                return Ok(Self::key_to_fh(self.alloc(snapshot)))
            }
            // Cache outdated. Need re-check.
            Some((snapshot, _)) => {
                log::debug!("cache outdated");
                Some(snapshot)
            }
            // No cache found.
            None => {
                log::debug!("cache miss");
                None
            }
        };

        // FIXME: Incremental fetching.
        let children_fields = inode::InodeAttr::SELECT_FIELDS
            .iter()
            .chain(&[DriveItemField::id, DriveItemField::name])
            .map(|field| field.raw_name())
            .collect::<Vec<_>>();
        let mut opt = ObjectOption::new()
            .select(&[
                // `id` is required, or we'll get 400 Bad Request.
                DriveItemField::id,
                DriveItemField::c_tag,
                DriveItemField::children,
            ])
            .expand(DriveItemField::children, Some(&children_fields));
        if let Some(prev) = &prev_snapshot {
            opt = opt.if_none_match(&prev.c_tag);
        }
        let ret = onedrive
            .get_item_with_option(ItemLocation::from_id(item_id), opt)
            .await?;
        let fetch_time = Instant::now();

        let dir_item = match ret {
            Some(item) => item,
            None => {
                // Content not changed. Reuse the cache.
                log::debug!("cache not modified, refresh");
                let prev_snapshot = prev_snapshot.unwrap();
                self.lru_cache
                    .lock()
                    .unwrap()
                    .insert(item_id.clone(), (prev_snapshot.clone(), fetch_time));
                return Ok(Self::key_to_fh(self.alloc(prev_snapshot)));
            }
        };

        let c_tag = dir_item.c_tag.unwrap();

        let mut entries = Vec::new();
        for item in dir_item.children.unwrap() {
            let child_attr = inode::InodeAttr::parse_item(&item).expect("Invalid DriveItem");
            let child_id = item.id.unwrap();
            inode_pool.touch(&child_id, child_attr.clone());
            entries.push(DirEntry {
                item_id: child_id,
                name: item.name.unwrap().into(),
                attr: child_attr,
            });
        }

        let name_map = entries
            .iter()
            .enumerate()
            .map(|(idx, ent)| (ent.name.to_str().unwrap().to_owned(), idx))
            .collect();

        let snapshot = Arc::new(DirSnapshot {
            c_tag,
            entries,
            name_map,
        });

        self.lru_cache
            .lock()
            .unwrap()
            .insert(item_id.clone(), (snapshot.clone(), fetch_time));
        Ok(Self::key_to_fh(self.alloc(snapshot)))
    }

    pub fn close(&self, fh: u64) -> Result<()> {
        if self.opened_handles.remove(Self::fh_to_key(fh)) {
            Ok(())
        } else {
            Err(Error::InvalidHandle(fh))
        }
    }

    pub async fn read(&self, fh: u64, offset: u64) -> Result<impl AsRef<[DirEntry]>> {
        let snapshot = self
            .opened_handles
            .get(Self::fh_to_key(fh))
            .ok_or(Error::InvalidHandle(fh))?
            .clone();

        // FIXME: Avoid copy.
        Ok(snapshot.entries[offset as usize..].to_owned())
    }

    /// Lookup name of a directory in cache and return DirEntry and TTL.
    ///
    /// `None` for cache miss.
    /// `Some(None) for not found.
    /// `Some(Some(_))` for found.
    pub async fn lookup(
        &self,
        parent_item_id: &ItemId,
        name: &str,
    ) -> Option<Option<(DirEntry, Duration)>> {
        let mut cache = self.lru_cache.lock().unwrap();
        if let Some((snapshot, last_fetch_time)) = cache.get_mut(parent_item_id) {
            if let Some(ttl) = self.config.cache_ttl.checked_sub(last_fetch_time.elapsed()) {
                let ret = snapshot
                    .name_map
                    .get(name)
                    .map(|&idx| (snapshot.entries[idx].clone(), ttl));
                return Some(ret);
            }
        }
        None
    }
}
