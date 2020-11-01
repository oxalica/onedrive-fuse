use crate::vfs::{
    dir,
    error::{Error, Result},
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
    time::SystemTime,
};

#[derive(Debug, Deserialize)]
pub struct Config {
    dead_lru_cache_size: usize,
}

#[derive(Debug, Clone)]
pub struct InodeAttr {
    pub parent_item_id: Option<ItemId>,
    pub size: u64,
    pub mtime: SystemTime,
    pub crtime: SystemTime,
    pub is_directory: bool,
}

impl InodeAttr {
    pub const SELECT_FIELDS: &'static [DriveItemField] = &[
        DriveItemField::root, // To verify `parent_reference`.
        DriveItemField::parent_reference,
        DriveItemField::size,
        DriveItemField::last_modified_date_time,
        DriveItemField::created_date_time,
        DriveItemField::folder,
    ];

    async fn fetch(loc: ItemLocation<'_>, onedrive: &OneDrive) -> Result<(ItemId, InodeAttr)> {
        let item = onedrive
            .get_item_with_option(
                loc,
                ObjectOption::new()
                    .select(&[DriveItemField::id])
                    .select(Self::SELECT_FIELDS),
            )
            .await?
            .expect("No If-None-Match");
        let attr = Self::parse_item(&item)
            .unwrap_or_else(|| panic!("Cannot parse inode attrs: {:?}", item));
        Ok((item.id.unwrap(), attr))
    }

    pub fn parse_item(item: &DriveItem) -> Option<InodeAttr> {
        fn parse_time(s: &str) -> Option<SystemTime> {
            humantime::parse_rfc3339(s).ok()
        }

        Some(InodeAttr {
            // https://docs.microsoft.com/en-us/graph/api/resources/itemreference?view=graph-rest-1.0
            parent_item_id: if item.root.is_some() {
                None
            } else {
                Some(ItemId(
                    item.parent_reference
                        .as_ref()?
                        .get("id")?
                        .as_str()?
                        .to_owned(),
                ))
            },
            size: item.size? as u64,
            mtime: parse_time(item.last_modified_date_time.as_deref()?)?,
            crtime: parse_time(item.created_date_time.as_deref()?)?,
            is_directory: item.folder.is_some(),
        })
    }
}

// Lock order: field order.
pub struct InodePool {
    alive: SyncMutex<AliveInodePool>,
    dead: SyncMutex<LruCache<ItemId, Arc<Inode>>>,
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

    /// Clear all InodeAttr cache.
    fn clear_attrs(&self) {
        for (_, node) in self.map.values() {
            *node.attr.lock().unwrap() = None;
        }
    }

    fn remove(&mut self, item_id: &ItemId) {
        if let Some(ino) = self.rev_map.remove(item_id) {
            // FIXME: We do not actually remove it now. Just clear the cache.
            log::warn!(
                "{:?} is removed from remote but still referenced locally",
                item_id,
            );
            *self.map.get_mut(&ino).unwrap().1.attr.lock().unwrap() = None;
        }
    }
}

struct Inode {
    item_id: ItemId,
    attr: SyncMutex<Option<InodeAttr>>,
}

impl InodePool {
    const ROOT_ID: u64 = fuse::FUSE_ROOT_ID;

    /// Initialize inode pool with root id to make operation on root nothing special.
    pub async fn new(config: Config, onedrive: &OneDrive) -> Result<Self> {
        // Should be small.
        static_assertions::const_assert_eq!(InodePool::ROOT_ID, 1);

        let (root_id, root_attr) = InodeAttr::fetch(ItemLocation::root(), onedrive).await?;

        let mut alive = AliveInodePool::new(Self::ROOT_ID);
        let root_node = Arc::new(Inode {
            item_id: root_id,
            attr: SyncMutex::new(Some(root_attr)),
        });
        alive.alloc(root_node);

        Ok(Self {
            alive: SyncMutex::new(alive),
            dead: SyncMutex::new(LruCache::new(config.dead_lru_cache_size)),
        })
    }

    /// Get inode by item_id without increasing its ref-count.
    /// Existing InodeAttr cache will be updated.
    pub fn touch(&self, item_id: &ItemId, attr: InodeAttr) {
        let alive = self.alive.lock().unwrap();
        match alive.get_ino(item_id) {
            Some(ino) => {
                let node = alive.get(ino).unwrap();
                *node.attr.lock().unwrap() = Some(attr);
            }
            None => {
                // Hold both mutex guards to avoid race.
                let mut dead = self.dead.lock().unwrap();
                let node = Arc::new(Inode {
                    item_id: item_id.clone(),
                    attr: SyncMutex::new(Some(attr)),
                });
                dead.insert(item_id.clone(), node);
            }
        }
    }

    /// Update InodeAttr of existing inode or allocate a new inode,
    /// also increase the reference count.
    fn acquire_or_alloc_with_attr(&self, item_id: &ItemId, attr: InodeAttr) -> u64 {
        let mut alive = self.alive.lock().unwrap();
        match alive.acquire_ino(item_id) {
            Some(ino) => {
                let node = alive.get(ino).unwrap();
                *node.attr.lock().unwrap() = Some(attr);
                ino
            }
            None => {
                // Not found in cache, allocate a new inode.
                let node = Arc::new(Inode {
                    item_id: item_id.clone(),
                    attr: SyncMutex::new(Some(attr)),
                });
                alive.alloc(node)
            }
        }
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
        dir_pool: &dir::DirPool,
        onedrive: &OneDrive,
    ) -> Result<(u64, InodeAttr)> {
        let parent_item_id = self.get_item_id(parent_ino)?;
        let child_name = cvt_filename(child_name)?;

        match dir_pool.lookup_cache(&parent_item_id, child_name.as_str()) {
            // Cache hit and item is not found.
            Some(None) => return Err(Error::NotFound),
            // Cache hit and item is found.
            Some(Some(ent)) => {
                let ino = self.acquire_or_alloc_with_attr(&ent.item_id, ent.attr.clone());
                return Ok((ino, ent.attr));
            }
            // Cache miss.
            None => {}
        }

        log::debug!("lookup: cache miss for: {}", child_name.as_str());

        // Fetch.
        let (item_id, attr) = InodeAttr::fetch(
            ItemLocation::child_of_id(&parent_item_id, child_name),
            onedrive,
        )
        .await?;

        let ino = self.acquire_or_alloc_with_attr(&item_id, attr.clone());
        Ok((ino, attr))
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

    /// Get InodeAttr of an existing inode.
    pub async fn get_attr(&self, ino: u64, onedrive: &OneDrive) -> Result<InodeAttr> {
        let node = self.alive.lock().unwrap().get(ino)?;
        if let Some(attr) = &*node.attr.lock().unwrap() {
            return Ok(attr.clone());
        }
        log::debug!("get_attr: cache miss");
        let (_, attr) = InodeAttr::fetch(ItemLocation::from_id(&node.item_id), onedrive).await?;
        *node.attr.lock().unwrap() = Some(attr.clone());
        Ok(attr)
    }

    /// Get item id from an existing inode.
    pub fn get_item_id(&self, ino: u64) -> Result<ItemId> {
        Ok(self.alive.lock().unwrap().get(ino)?.item_id.clone())
    }

    /// Clear all InodeAttr cache.
    pub fn clear_cache(&self) {
        log::trace!("Cache clear");
        let alive = self.alive.lock().unwrap();
        alive.clear_attrs();
        self.dead.lock().unwrap().clear();
    }

    /// Sync item changes from remote. Items not in cache are skipped.
    pub fn sync_items(&self, items: &[DriveItem]) {
        let mut alive = self.alive.lock().unwrap();
        let mut dead = self.dead.lock().unwrap();
        for item in items {
            let item_id = item.id.as_ref().unwrap();
            if item.deleted.is_some() {
                alive.remove(item_id);
                dead.remove(item_id);
            } else {
                let attr = InodeAttr::parse_item(item).unwrap();
                // We cannot use `touch` here, or we will enter a deadlock over `self.alive`.
                if let Some(ino) = alive.get_ino(item_id) {
                    *alive.get(ino).unwrap().attr.lock().unwrap() = Some(attr);
                }
            }
        }
    }
}

fn cvt_filename<'a>(name: &'a OsStr) -> Result<&'a FileName> {
    name.to_str()
        .and_then(FileName::new)
        .ok_or_else(|| Error::InvalidFileName(name.to_owned()))
}
