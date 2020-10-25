use crate::{
    util::de_duration_sec,
    vfs::{
        dir,
        error::{Error, Result},
    },
};
use lru_cache::LruCache;
use onedrive_api::{
    option::ObjectOption,
    resource::{DriveItem, DriveItemField},
    FileName, ItemId, ItemLocation, OneDrive,
};
use serde::Deserialize;
use std::{
    collections::hash_map::{Entry, HashMap},
    ffi::OsStr,
    sync::{Arc, Mutex as SyncMutex},
    time::{Duration, Instant},
};
use time::Timespec;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "de_duration_sec")]
    attr_cache_ttl: Duration,
    dead_lru_cache_size: usize,
}

// This should not hold any heap-allocation due to the requirement `Inode: Clear`.
#[derive(Debug, Clone, Copy)]
pub struct InodeAttr {
    pub size: u64,
    pub mtime: Timespec,
    pub crtime: Timespec,
    pub is_directory: bool,
}

impl InodeAttr {
    const SELECT_FIELDS: &'static [DriveItemField] = &[
        DriveItemField::id,
        DriveItemField::size,
        DriveItemField::last_modified_date_time,
        DriveItemField::created_date_time,
        DriveItemField::folder,
    ];

    async fn fetch(loc: ItemLocation<'_>, onedrive: &OneDrive) -> Result<(ItemId, InodeAttr)> {
        // The reply is quite small and we don't need to check changes with eTag.
        let item = onedrive
            .get_item_with_option(loc, ObjectOption::new().select(Self::SELECT_FIELDS))
            .await?
            .expect("No If-None-Match");
        Self::parse_drive_item(&item)
    }

    pub fn parse_drive_item(item: &DriveItem) -> Result<(ItemId, InodeAttr)> {
        fn parse_time(s: &str) -> Timespec {
            // FIXME
            time::strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z")
                .or_else(|_| time::strptime(s, "%Y-%m-%dT%H:%M:%S%z"))
                .unwrap_or_else(|err| panic!("Invalid time '{}': {}", s, err))
                .to_timespec()
        }

        let item_id = item.id.clone().unwrap();
        let attr = InodeAttr {
            size: item.size.unwrap() as u64,
            mtime: parse_time(item.last_modified_date_time.as_deref().unwrap()),
            crtime: parse_time(item.created_date_time.as_deref().unwrap()),
            is_directory: item.folder.is_some(),
        };
        Ok((item_id, attr))
    }
}

// Lock order: field order.
pub struct InodePool {
    alive: SyncMutex<AliveInodePool>,
    dead: SyncMutex<LruCache<ItemId, Arc<Inode>>>,
    config: Config,
}

struct AliveInodePool {
    inode_counter: u64,
    /// inode_id -> (reference_count, Inode)
    map: HashMap<u64, (u64, Arc<Inode>)>,
    rev_map: HashMap<ItemId, u64>,
}

impl AliveInodePool {
    fn new(init_inode: u64) -> Self {
        Self {
            inode_counter: init_inode,
            map: Default::default(),
            rev_map: Default::default(),
        }
    }

    /// Get the node by inode id.
    fn get(&self, ino: u64) -> Result<Arc<Inode>> {
        self.map
            .get(&ino)
            .map(|(_, node)| node.clone())
            .ok_or(Error::InvalidInode(ino))
    }

    /// Get inode id by item_id.
    fn get_ino(&self, item_id: &ItemId) -> Option<u64> {
        self.rev_map.get(item_id).copied()
    }

    /// Find an inode id by `item_id` and increase its reference count.
    fn acquire_ino(&mut self, item_id: &ItemId) -> Option<u64> {
        let ino = self.get_ino(item_id)?;
        self.map.get_mut(&ino).unwrap().0 += 1;
        Some(ino)
    }

    /// Allocate a new inode with 1 reference count and return the inode id.
    fn alloc(&mut self, node: Arc<Inode>) -> u64 {
        let ino = self.inode_counter;
        self.inode_counter += 1;
        assert!(self.rev_map.insert(node.item_id.clone(), ino).is_none());
        assert!(self.map.insert(ino, (1, node)).is_none());
        ino
    }

    /// Decrease reference count by `count` and optionally remove the entry.
    /// Return the node if it is removed.
    fn free(&mut self, ino: u64, count: u64) -> Result<Option<Arc<Inode>>> {
        match self.map.entry(ino) {
            Entry::Vacant(_) => Err(Error::InvalidInode(ino)),
            Entry::Occupied(mut ent) => {
                assert!(count <= ent.get_mut().0);
                if ent.get_mut().0 == count {
                    let (_, node) = ent.remove();
                    assert!(self.rev_map.remove(&node.item_id).is_some());
                    Ok(Some(node))
                } else {
                    ent.get_mut().0 -= count;
                    Ok(None)
                }
            }
        }
    }
}

struct Inode {
    item_id: ItemId,
    attr: Mutex<(InodeAttr, Instant)>,
}

impl InodePool {
    const ROOT_ID: u64 = fuse::FUSE_ROOT_ID;

    /// Initialize inode pool with root id to make operation on root nothing special.
    pub async fn new(config: Config, onedrive: &OneDrive) -> Result<Self> {
        // Should be small.
        static_assertions::const_assert_eq!(InodePool::ROOT_ID, 1);

        let (root_id, root_attr) = InodeAttr::fetch(ItemLocation::root(), onedrive).await?;
        let fetch_time = Instant::now();

        let mut alive = AliveInodePool::new(Self::ROOT_ID);
        let root_node = Arc::new(Inode {
            item_id: root_id,
            attr: Mutex::new((root_attr, fetch_time)),
        });
        alive.alloc(root_node);

        Ok(Self {
            alive: SyncMutex::new(alive),
            dead: SyncMutex::new(LruCache::new(config.dead_lru_cache_size)),
            config,
        })
    }

    /// Get inode by item_id without increasing its ref-count.
    /// Inode data may or may not be cached.
    ///
    /// This is used in `readdir`.
    pub async fn touch(&self, item_id: &ItemId, attr: InodeAttr, fetch_time: Instant) {
        let node = {
            let alive = self.alive.lock().unwrap();
            match alive.get_ino(item_id) {
                Some(ino) => alive.get(ino).unwrap(),
                None => {
                    // Hold both mutex guards to avoid race.
                    let mut dead = self.dead.lock().unwrap();
                    let node = Arc::new(Inode {
                        item_id: item_id.clone(),
                        attr: Mutex::new((attr, fetch_time)),
                    });
                    dead.insert(item_id.clone(), node);
                    return;
                }
            }
        };
        // We cannot use `await` when holding guard.
        let mut cache = node.attr.lock().await;
        // Only update if later.
        if cache.1 < fetch_time {
            *cache = (attr, fetch_time);
        }
    }

    /// Update InodeAttr of existing inode or allocate a new inode,
    /// also increase the reference count.
    async fn acquire_or_alloc_with_attr(
        &self,
        item_id: &ItemId,
        attr: InodeAttr,
        fetch_time: Instant,
    ) -> u64 {
        let (ino, node) = {
            let mut alive = self.alive.lock().unwrap();
            match alive.acquire_ino(item_id) {
                Some(ino) => (ino, alive.get(ino).unwrap()),
                None => {
                    // Not found in cache, allocate a new inode.
                    let node = Arc::new(Inode {
                        item_id: item_id.clone(),
                        attr: Mutex::new((attr, fetch_time)),
                    });
                    let ino = alive.alloc(node);
                    return ino;
                }
            }
        };

        // We cannot use `await` when holding mutex guard above.
        let mut cache = node.attr.lock().await;
        // Only update if later.
        if cache.1 < fetch_time {
            *cache = (attr, fetch_time);
        }
        ino
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
        dir_pool: &dir::DirPool,
        onedrive: &OneDrive,
    ) -> Result<(u64, InodeAttr, Duration)> {
        let parent_item_id = self.get_item_id(parent_ino)?;
        let child_name = cvt_filename(child_name)?;

        match dir_pool.lookup(&parent_item_id, child_name.as_str()).await {
            // Cache hit but item not found.
            Some(None) => return Err(Error::NotFound),
            // Cache hit and item found.
            Some(Some((ent, ttl))) => {
                let ino = self
                    .acquire_or_alloc_with_attr(&ent.item_id, ent.attr, Instant::now() - ttl)
                    .await;
                return Ok((ino, ent.attr, ttl));
            }
            // Cache miss.
            None => {}
        }

        // Fetch.
        let (item_id, attr) = InodeAttr::fetch(
            ItemLocation::child_of_id(&parent_item_id, child_name),
            onedrive,
        )
        .await?;
        let fetch_time = Instant::now();

        let ino = self
            .acquire_or_alloc_with_attr(&item_id, attr, fetch_time)
            .await;
        let ttl = self.config.attr_cache_ttl;
        Ok((ino, attr, ttl))
    }

    /// Decrease reference count of an inode by `count`.
    /// Return if it is freed.
    pub async fn free(&self, ino: u64, count: u64) -> Result<bool> {
        let mut alive = self.alive.lock().unwrap();
        if let Some(node) = alive.free(ino, count)? {
            // When freed, put it into cache to allow reusing metadata.
            // Hold both mutex guards to avoid race.
            self.dead.lock().unwrap().insert(node.item_id.clone(), node);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn get_attr(&self, ino: u64, onedrive: &OneDrive) -> Result<(InodeAttr, Duration)> {
        let node = self.alive.lock().unwrap().get(ino)?;
        let mut attr = node.attr.lock().await;

        // Check if cache is outdated.
        if let Some(ttl) = self.config.attr_cache_ttl.checked_sub(attr.1.elapsed()) {
            return Ok((attr.0, ttl));
        }

        // Cache outdated. Hold the mutex during the request.
        log::debug!("cache miss");
        let (_, new_attr) =
            InodeAttr::fetch(ItemLocation::from_id(&node.item_id), onedrive).await?;
        // Refresh cache.
        *attr = (new_attr, Instant::now());

        let ttl = self.config.attr_cache_ttl;
        Ok((new_attr, ttl))
    }

    pub fn get_item_id(&self, ino: u64) -> Result<ItemId> {
        Ok(self.alive.lock().unwrap().get(ino)?.item_id.clone())
    }
}

fn cvt_filename<'a>(name: &'a OsStr) -> Result<&'a FileName> {
    name.to_str()
        .and_then(FileName::new)
        .ok_or_else(|| Error::InvalidFileName(name.to_owned()))
}
