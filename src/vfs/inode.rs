use crate::vfs::{
    dir,
    error::{Error, Result},
};
use onedrive_api::{
    option::ObjectOption,
    resource::{DriveItem, DriveItemField},
    FileName, ItemId, ItemLocation, OneDrive,
};
use reqwest::StatusCode;
use serde::Deserialize;
use std::{
    collections::hash_map::{Entry, HashMap},
    ffi::OsStr,
    sync::Mutex as SyncMutex,
    time::SystemTime,
};

#[derive(Debug, Deserialize)]
pub struct Config {}

#[derive(Debug, Clone)]
pub struct InodeAttr {
    pub size: u64,
    pub mtime: SystemTime,
    pub crtime: SystemTime,
    pub is_directory: bool,
}

impl InodeAttr {
    pub const ATTR_SELECT_FIELDS: &'static [DriveItemField] = &[
        DriveItemField::size,
        DriveItemField::last_modified_date_time,
        DriveItemField::created_date_time,
        DriveItemField::folder,
    ];

    pub fn parse_item(item: &DriveItem) -> Option<InodeAttr> {
        fn parse_time(s: &str) -> Option<SystemTime> {
            humantime::parse_rfc3339(s).ok()
        }

        Some(InodeAttr {
            size: item.size? as u64,
            mtime: parse_time(item.last_modified_date_time.as_deref()?)?,
            crtime: parse_time(item.created_date_time.as_deref()?)?,
            is_directory: item.folder.is_some(),
        })
    }

    pub fn parse_parent_id(item: &DriveItem) -> Option<ItemId> {
        // https://docs.microsoft.com/en-us/graph/api/resources/itemreference?view=graph-rest-1.0
        let id = item.parent_reference.as_ref()?.get("id")?.as_str()?;
        Some(ItemId(id.to_owned()))
    }
}

pub struct InodePool {
    alive: SyncMutex<AliveInodePool>,
    root_attr: SyncMutex<Option<InodeAttr>>,
}

struct AliveInodePool {
    inode_counter: u64,
    /// ino -> (reference_count, item_id)
    map: HashMap<u64, (u64, ItemId)>,
    /// item_id -> ino
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

    /// Get the item_id by inode id.
    fn get(&self, ino: u64) -> Result<ItemId> {
        self.map
            .get(&ino)
            .map(|(_, item_id)| item_id.clone())
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
    fn alloc(&mut self, item_id: ItemId) -> u64 {
        let ino = self.inode_counter;
        self.inode_counter += 1;
        assert!(self.rev_map.insert(item_id.clone(), ino).is_none());
        assert!(self.map.insert(ino, (1, item_id)).is_none());
        ino
    }

    /// Decrease reference count by `count` and optionally remove the entry.
    /// Return the node if it is removed.
    fn free(&mut self, ino: u64, count: u64) -> Result<Option<ItemId>> {
        match self.map.entry(ino) {
            Entry::Vacant(_) => Err(Error::InvalidInode(ino)),
            Entry::Occupied(mut ent) => {
                assert!(count <= ent.get_mut().0);
                if ent.get_mut().0 == count {
                    let (_, item_id) = ent.remove();
                    assert!(self.rev_map.remove(&item_id).is_some());
                    Ok(Some(item_id))
                } else {
                    ent.get_mut().0 -= count;
                    Ok(None)
                }
            }
        }
    }
}

impl InodePool {
    const ROOT_ID: u64 = fuse::FUSE_ROOT_ID;

    /// Initialize inode pool with root id to make operation on root nothing special.
    pub async fn new(_config: Config, onedrive: &OneDrive) -> anyhow::Result<Self> {
        // Should be small.
        static_assertions::const_assert_eq!(InodePool::ROOT_ID, 1);

        let root_item = onedrive
            .get_item_with_option(
                ItemLocation::root(),
                ObjectOption::new()
                    .select(&[DriveItemField::id])
                    .select(InodeAttr::ATTR_SELECT_FIELDS),
            )
            .await?
            .expect("No If-None-Match");
        let root_attr = InodeAttr::parse_item(&root_item).unwrap();

        let mut alive = AliveInodePool::new(Self::ROOT_ID);
        // Never freed.
        alive.alloc(root_item.id.unwrap());

        Ok(Self {
            alive: SyncMutex::new(alive),
            root_attr: SyncMutex::new(Some(root_attr)),
        })
    }

    /// Update InodeAttr of existing inode or allocate a new inode,
    /// also increase the reference count.
    fn acquire_or_alloc(&self, item_id: &ItemId) -> u64 {
        let mut alive = self.alive.lock().unwrap();
        match alive.acquire_ino(item_id) {
            Some(ino) => ino,
            None => alive.alloc(item_id.clone()),
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
                let ino = self.acquire_or_alloc(&ent.item_id);
                return Ok((ino, ent.attr));
            }
            // Cache miss.
            None => {}
        }

        log::debug!("lookup: cache miss for: {}", child_name.as_str());

        // Fetch.
        match onedrive
            .get_item_with_option(
                ItemLocation::child_of_id(&parent_item_id, child_name),
                ObjectOption::new()
                    .select(&[DriveItemField::id, DriveItemField::name])
                    .select(InodeAttr::ATTR_SELECT_FIELDS),
            )
            .await
        {
            Ok(item) => {
                let item = item.expect("No If-None-Match");
                let attr = InodeAttr::parse_item(&item).unwrap();
                let item_id = item.id.unwrap();

                dir_pool.cache_child(
                    parent_item_id,
                    Ok(dir::DirEntry {
                        name: item.name.unwrap(),
                        item_id: item_id.clone(),
                        attr: attr.clone(),
                    }),
                );

                let ino = self.acquire_or_alloc(&item_id);
                Ok((ino, attr))
            }
            Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                dir_pool.cache_child(parent_item_id, Err(child_name.as_str().to_owned()));
                Err(err.into())
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Decrease reference count of an inode by `count`.
    /// Return if it is freed.
    pub async fn free(&self, ino: u64, count: u64) -> Result<bool> {
        let mut alive = self.alive.lock().unwrap();
        Ok(alive.free(ino, count)?.is_some())
    }

    /// Get InodeAttr of an existing inode.
    pub async fn get_attr(
        &self,
        ino: u64,
        dir_pool: &dir::DirPool,
        onedrive: &OneDrive,
    ) -> Result<InodeAttr> {
        let item_id = self.get_item_id(ino)?;
        if ino == Self::ROOT_ID {
            if let Some(attr) = &*self.root_attr.lock().unwrap() {
                return Ok(attr.clone());
            }
        } else if let Some(attr) = dir_pool.get_item_attr_cache(&item_id) {
            return Ok(attr);
        }

        log::debug!("get_attr: cache miss for {:?}", item_id);

        let item = onedrive
            .get_item_with_option(
                ItemLocation::from_id(&item_id),
                ObjectOption::new()
                    .select(&[
                        DriveItemField::name,
                        DriveItemField::root,
                        DriveItemField::parent_reference,
                    ])
                    .select(InodeAttr::ATTR_SELECT_FIELDS),
            )
            .await?
            .expect("No If-None-Match");
        let attr = InodeAttr::parse_item(&item).unwrap();

        match InodeAttr::parse_parent_id(&item) {
            Some(parent_id) => {
                dir_pool.cache_child(
                    parent_id,
                    Ok(dir::DirEntry {
                        name: item.name.unwrap(),
                        item_id,
                        attr: attr.clone(),
                    }),
                );
            }
            // Root directory.
            None => {
                assert_eq!(ino, Self::ROOT_ID);
                *self.root_attr.lock().unwrap() = Some(attr.clone());
            }
        }

        Ok(attr)
    }

    /// Get item id from an existing inode.
    pub fn get_item_id(&self, ino: u64) -> Result<ItemId> {
        Ok(self.alive.lock().unwrap().get(ino)?)
    }

    /// Clear all cache.
    pub fn clear_cache(&self) {
        *self.root_attr.lock().unwrap() = None;
    }

    /// Sync item changes from remote.
    pub fn sync_items(&self, items: &[DriveItem]) {
        // We only concern about the root item.
        let mut root_attr = self.root_attr.lock().unwrap();
        for item in items {
            if item.root.is_some() {
                assert!(item.deleted.is_none());
                let attr = InodeAttr::parse_item(item).unwrap();
                *root_attr = Some(attr);
            }
        }
    }
}

fn cvt_filename(name: &OsStr) -> Result<&FileName> {
    name.to_str()
        .and_then(FileName::new)
        .ok_or_else(|| Error::InvalidFileName(name.to_owned()))
}
