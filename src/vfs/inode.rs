//! Directory hierarachy and item attributes.
use crate::vfs::error::{Error, Result};
use indexmap::IndexMap;
use lru_cache::LruCache;
use onedrive_api::{
    option::{CollectionOption, ObjectOption},
    resource::{DriveItem, DriveItemField},
    ItemId, ItemLocation, OneDrive,
};
use serde::Deserialize;
use std::{
    sync::{Arc, Mutex as SyncMutex, Weak},
    time::SystemTime,
};
use tokio::sync::{Mutex, RwLock};
use weak_table::WeakKeyHashMap;

#[derive(Debug, Clone)]
pub struct InodeAttr {
    pub size: u64,
    pub mtime: SystemTime,
    pub crtime: SystemTime,
    pub is_directory: bool,
}

impl InodeAttr {
    pub const SELECT_FIELDS: &'static [DriveItemField] = &[
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
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub item_id: ItemId,
    pub name: String,
    pub attr: InodeAttr,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    lru_cache_size: usize,
}

// Lock order: field order.
pub struct InodePool {
    // Any fetching requires a read permit, while a sync call requires a write permit.
    // It is used for avoid race between fetching and delta response on the same item.
    fetch_guard: RwLock<()>,
    meta_map: SyncMutex<LruCache<ItemId, ItemMetaSlot>>,
    // Item id -> (parent id, index of children).
    parent_map: SyncMutex<WeakKeyHashMap<Weak<str>, (ItemId, usize)>>,
    config: Config,
}

type ItemMetaSlot = Arc<Mutex<Option<ItemMeta>>>;

#[derive(Debug)]
struct ItemMeta {
    attr: InodeAttr,
    // Can only be `Some` for directories.
    children: Mutex<Option<DirChildren>>,
}

// Name -> item id.
type DirChildren = IndexMap<String, Arc<str>>;

impl InodePool {
    pub fn new(config: Config) -> Self {
        Self {
            fetch_guard: RwLock::new(()),
            meta_map: SyncMutex::new(LruCache::new(config.lru_cache_size)),
            parent_map: SyncMutex::new(WeakKeyHashMap::new()),
            config,
        }
    }

    fn get_or_new_meta_slot(&self, item_id: &ItemId) -> ItemMetaSlot {
        let mut map = self.meta_map.lock().unwrap();
        match map.get_mut(item_id) {
            Some(slot) => slot.clone(),
            None => {
                let slot = Arc::new(Mutex::new(None));
                map.insert(item_id.clone(), slot.clone());
                slot
            }
        }
    }

    /// Get item meta from cache, or fetch and cache them.
    /// It fetch item attribute and children attributes (for directories) together if possible.
    async fn get_or_fetch_attr(
        &self,
        item_id: &ItemId,
        onedrive: &OneDrive,
    ) -> Result<ItemMetaSlot> {
        let meta_slot = self.get_or_new_meta_slot(item_id);

        // Lock the meta slot during the fetch to prevent multiple identical requests.
        let mut meta_guard = meta_slot.lock().await;
        if meta_guard.is_some() {
            drop(meta_guard);
            return Ok(meta_slot);
        }

        log::trace!("Fetching attribute of {:?}", item_id);

        let children_fields = InodeAttr::SELECT_FIELDS
            .iter()
            .chain(&[
                DriveItemField::id,
                DriveItemField::name,
                DriveItemField::folder,
            ])
            .map(|field| field.raw_name())
            .collect::<Vec<_>>();
        let opt = ObjectOption::new()
            .select(&[
                // `id` is required, or we'll get 400 Bad Request.
                DriveItemField::id,
                DriveItemField::children,
            ])
            .select(InodeAttr::SELECT_FIELDS)
            .expand(DriveItemField::children, Some(&children_fields));
        let item = onedrive
            .get_item_with_option(ItemLocation::from_id(item_id), opt)
            .await?
            .expect("No If-None-Match");
        let attr = InodeAttr::parse_item(&item).expect("Invalid attrs");

        let children = {
            if !attr.is_directory {
                Mutex::new(None)
            } else {
                let children_count = (|| item.folder.as_ref()?.get("childCount")?.as_u64())()
                    .expect("Missing folder.childCount");
                let children = item.children.expect("Missing children field");
                // FIXME: got length == childCount - 1 for root directory.
                if children.len() as u64 == children_count
                    && children.len() < self.config.lru_cache_size
                {
                    Mutex::new(Some(self.cache_children(item_id, children).await))
                } else {
                    log::debug!(
                        "Too many children of {:?}, total={}, got={}, cache_size={}",
                        item_id,
                        children_count,
                        children.len(),
                        self.config.lru_cache_size
                    );
                    // FIXME: A single request cannot get all children.
                    // Maybe save as partial result?
                    Mutex::new(None)
                }
            }
        };

        *meta_guard = Some(ItemMeta { attr, children });
        drop(meta_guard);
        Ok(meta_slot)
    }

    async fn get_or_fetch_children(
        &self,
        item_id: &ItemId,
        onedrive: &OneDrive,
    ) -> Result<ItemMetaSlot> {
        let meta_slot = self.get_or_fetch_attr(item_id, onedrive).await?;
        let meta_guard = meta_slot.lock().await;
        let meta = meta_guard.as_ref().expect("Already populated");
        if !meta.attr.is_directory {
            return Err(Error::NotADirectory);
        }

        let mut children = meta.children.lock().await;
        if children.is_some() {
            drop(children);
            drop(meta_guard);
            return Ok(meta_slot);
        }

        log::trace!("Fetching children of {:?}", item_id);

        // TODO: Incremental fetch?
        let ret_children = onedrive
            .list_children_with_option(
                ItemLocation::from_id(item_id),
                CollectionOption::new()
                    .select(&[DriveItemField::id, DriveItemField::name])
                    .select(InodeAttr::SELECT_FIELDS),
            )
            .await?
            .expect("No If-None-Match")
            .fetch_all(onedrive)
            .await?;
        let ret_children = self.cache_children(item_id, ret_children).await;

        *children = Some(ret_children);

        drop(children);
        drop(meta_guard);
        return Ok(meta_slot);
    }

    async fn cache_children(&self, item_id: &ItemId, children: Vec<DriveItem>) -> DirChildren {
        log::trace!("Cache {} children of {:?}", children.len(), item_id);
        let mut map = self.meta_map.lock().unwrap();
        let mut parent_map = self.parent_map.lock().unwrap();

        children
            .into_iter()
            .enumerate()
            .map(|(child_idx, child_item)| {
                let child_meta = ItemMeta {
                    attr: InodeAttr::parse_item(&child_item).expect("Invalid attrs"),
                    children: Mutex::new(None),
                };
                let child_name = child_item.name.expect("Missing name");
                let child_id = child_item.id.expect("Missing id");
                assert_ne!(&child_id, item_id, "No cycle");
                // Cache child meta.
                map.insert(child_id.clone(), Arc::new(Mutex::new(Some(child_meta))));

                // Register parent reference.
                let child_id: Arc<str> = child_id.0.into();
                parent_map.insert(child_id.clone(), (item_id.clone(), child_idx));
                (child_name, child_id)
            })
            .collect()
    }

    /// Get attribute of an item.
    pub async fn get_attr(&self, item_id: &ItemId, onedrive: &OneDrive) -> Result<InodeAttr> {
        let _guard = self.fetch_guard.read().await;
        Ok(self
            .get_or_fetch_attr(item_id, onedrive)
            .await?
            .lock()
            .await
            .as_ref()
            .expect("Already populated")
            .attr
            .clone())
    }

    /// Lookup a child by name of an directory item.
    pub async fn lookup(
        &self,
        parent_id: &ItemId,
        child_name: &str,
        onedrive: &OneDrive,
    ) -> Result<ItemId> {
        let _guard = self.fetch_guard.read().await;
        let slot = self.get_or_fetch_children(parent_id, onedrive).await?;
        let meta = slot.lock().await;
        let children = meta
            .as_ref()
            .expect("Already populated")
            .children
            .lock()
            .await;
        let children = children.as_ref().expect("Already populated");
        Ok(ItemId(String::from(
            &**children.get(child_name).ok_or(Error::NotFound)?,
        )))
    }

    /// Read entries of a directory.
    pub async fn read_dir(
        &self,
        parent_id: &ItemId,
        offset: u64,
        count: usize,
        onedrive: &OneDrive,
    ) -> Result<Vec<DirEntry>> {
        let _guard = self.fetch_guard.read().await;
        let slot = self.get_or_fetch_children(parent_id, onedrive).await?;
        let meta = slot.lock().await;
        let children = meta
            .as_ref()
            .expect("Already populated")
            .children
            .lock()
            .await;
        let children = children.as_ref().expect("Already populated");

        let mut entries = Vec::with_capacity(count);
        let l = (offset as usize).min(children.len());
        let r = (l + count).min(children.len());
        for i in l..r {
            let (name, id) = children.get_index(i).unwrap();
            let item_id = ItemId(String::from(&**id));
            assert_ne!(&item_id, parent_id, "No cycle");
            let attr = self.get_attr(&item_id, onedrive).await?;
            entries.push(DirEntry {
                name: name.clone(),
                item_id,
                attr,
            });
        }
        Ok(entries)
    }

    /// Clear all cache.
    pub async fn clear_cache(&self) {
        let _guard = self.fetch_guard.write().await;
        self.meta_map.lock().unwrap().clear();
        // Parent map will be automatically clear since it's a weak map.
    }

    /// Sync item changes from remote. Items not in cache are skipped.
    pub async fn sync_items(&self, updated: &[DriveItem]) {
        let _guard = self.fetch_guard.write().await;
        let mut map = self.meta_map.lock().unwrap();
        let mut parent_map = self.parent_map.lock().unwrap();

        for item in updated {
            let item_id = item.id.as_ref().unwrap();

            // 1. Handle attribute update.
            let new_attr = InodeAttr::parse_item(&item).unwrap();
            if let Some(meta) = map.get_mut(item_id) {
                if let Some(meta) = &mut *meta.try_lock().expect("Fetch guard is held") {
                    meta.attr = new_attr;
                }
            }

            // 2. Root directory cannot be moved or renamed. Skip.
            if item.root.is_some() {
                continue;
            }

            let new_parent_id = (|| {
                let id = item.parent_reference.as_ref()?.get("id")?.as_str()?;
                Some(ItemId(id.to_owned()))
            })()
            .expect("Missing new parent for non-root item");
            let new_name = item.name.clone().expect("Missing name field");

            // 3. If the item is cached in some directory.
            if let Some((old_parent_id, old_idx)) = parent_map.get(&*item_id.0).cloned() {
                let parent_meta = map
                    .get_mut(&old_parent_id)
                    .expect("Parent reference lives")
                    .try_lock()
                    .expect("Fetch guard is held");
                let parent_meta = parent_meta.as_ref().expect("Parent reference lives");
                let mut parent_children = parent_meta
                    .children
                    .try_lock()
                    .expect("Fetch guard is held");
                let parent_children = parent_children.as_mut().expect("Parent reference lives");

                // 3.1. Do nothing if neither parent or name is modified.
                if old_parent_id == new_parent_id
                    && parent_children.get_index(old_idx).unwrap().0 == &new_name
                {
                    continue;
                }

                // 3.2. Remove the old entry.
                log::debug!(
                    "Remove {:?} from cached directory {:?}",
                    item_id,
                    old_parent_id,
                );
                if old_idx + 1 != parent_children.len() {
                    // Maintain reverse reference of the last entry to be swapped.
                    parent_map[&*parent_children.last().unwrap().1].1 = old_idx;
                }
                parent_children.swap_remove_index(old_idx);
            }

            // 4. If the new parent directory is cached.
            if let Some(meta) = map.get_mut(&new_parent_id) {
                if let Some(meta) = &*meta.try_lock().expect("Fetch guard is held") {
                    if let Some(children) =
                        &mut *meta.children.try_lock().expect("Fetch guard is held")
                    {
                        // 4.1. Add a new entry.
                        log::debug!(
                            "Add {:?} into cached directory {:?}",
                            item_id,
                            new_parent_id,
                        );
                        let id: Arc<str> = (&*item_id.0).into();
                        let child_idx = children.len();
                        children.insert(new_name, id.clone());
                        parent_map.insert(id, (new_parent_id, child_idx));
                    }
                }
            }
        }
    }
}
