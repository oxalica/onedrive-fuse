use crate::vfs::{error::Result, inode};
use lru_cache::LruCache;
use onedrive_api::{
    option::ObjectOption,
    resource::{DriveItem, DriveItemField},
    ItemId, ItemLocation, OneDrive, Tag,
};
use serde::Deserialize;
use std::{collections::HashMap, convert::TryFrom as _, sync::Mutex as SyncMutex};

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub item_id: ItemId,
    pub name: String,
    pub attr: inode::InodeAttr,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    lru_cache_size: usize,
}

pub struct DirPool {
    cache: SyncMutex<LruCache<ItemId, DirSnapshot>>,
}

struct DirSnapshot {
    c_tag: Tag,
    entries: Vec<DirEntry>,
    /// name -> index of `entries`
    name_map: HashMap<String, usize>,
}

impl DirSnapshot {
    fn new(c_tag: Tag, iter: impl IntoIterator<Item = DirEntry>) -> Self {
        let entries = iter.into_iter().collect::<Vec<_>>();
        let name_map = entries
            .iter()
            .enumerate()
            .map(|(idx, ent)| (ent.name.clone(), idx))
            .collect();
        Self {
            c_tag,
            entries,
            name_map,
        }
    }

    fn read(&self, offset: u64, count: usize) -> impl AsRef<[DirEntry]> {
        let len = self.entries.len();
        let start = usize::try_from(offset).unwrap_or(len).min(len);
        let end = start.checked_add(count).unwrap_or(len).min(len);
        // TODO: Avoid clone.
        self.entries[start..end].to_owned()
    }

    fn lookup(&self, name: &str) -> Option<&DirEntry> {
        Some(&self.entries[*self.name_map.get(name)?])
    }
}

impl DirPool {
    pub fn new(config: Config) -> Self {
        Self {
            cache: SyncMutex::new(LruCache::new(config.lru_cache_size)),
        }
    }

    pub async fn read(
        &self,
        parent_id: &ItemId,
        offset: u64,
        count: usize,
        inode_pool: &inode::InodePool,
        onedrive: &OneDrive,
    ) -> Result<impl AsRef<[DirEntry]>> {
        if let Some(dir) = self.cache.lock().unwrap().get_mut(parent_id) {
            return Ok(dir.read(offset, count));
        }

        // FIXME: Incremental fetching.
        let children_fields = inode::InodeAttr::SELECT_FIELDS
            .iter()
            .chain(&[DriveItemField::id, DriveItemField::name])
            .map(|field| field.raw_name())
            .collect::<Vec<_>>();
        let opt = ObjectOption::new()
            .select(&[
                // `id` is required, or we'll get 400 Bad Request.
                DriveItemField::id,
                // Used to check if the content is updated in `sync_items`.
                DriveItemField::c_tag,
                DriveItemField::children,
            ])
            .expand(DriveItemField::children, Some(&children_fields));
        let dir_item = onedrive
            .get_item_with_option(ItemLocation::from_id(parent_id), opt)
            .await?
            .expect("No If-None-Match");

        let snapshot = DirSnapshot::new(
            dir_item.c_tag.unwrap(),
            dir_item.children.unwrap().into_iter().map(|item| {
                let attr = inode::InodeAttr::parse_item(&item).expect("Invalid DriveItem");
                let name = item.name.unwrap();
                let item_id = item.id.unwrap();
                inode_pool.touch(&item_id, attr.clone());
                DirEntry {
                    item_id,
                    name,
                    attr,
                }
            }),
        );

        let ret = snapshot.read(offset, count);

        self.cache
            .lock()
            .unwrap()
            .insert(parent_id.clone(), snapshot);

        Ok(ret)
    }

    /// Lookup name of a directory in cache and return ItemId.
    ///
    /// `None` for cache miss.
    /// `Some(None) for not found.
    /// `Some(Some(_))` for found.
    pub fn lookup(&self, parent_id: &ItemId, name: &str) -> Option<Option<ItemId>> {
        let mut cache = self.cache.lock().unwrap();
        Some(
            cache
                .get_mut(parent_id)?
                .lookup(name)
                .map(|ent| ent.item_id.clone()),
        )
    }

    /// Clear all cache.
    pub fn clear_cache(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Sync item changes from remote. Items not in cache are skipped.
    pub fn sync_items(&self, items: &[DriveItem]) {
        let mut cache = self.cache.lock().unwrap();
        for item in items {
            // If a file `/a/b/c` is modified or renamed.
            // All ancenters `/`, `/a` and `/a/b` will also received an change.
            // So we can simply consider only directory changes here.
            if item.folder.is_some() {
                let item_id = item.id.as_ref().unwrap();
                if let Some(dir) = cache.get_mut(&item_id) {
                    if &dir.c_tag != item.c_tag.as_ref().unwrap() {
                        log::trace!("Drop directory cache of {:?}", item_id);
                        cache.remove(&item_id);
                    }
                }
            }
        }
    }
}
