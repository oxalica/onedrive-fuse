use crate::vfs::{error::Result, inode};
use lru_cache::LruCache;
use onedrive_api::{
    option::ObjectOption,
    resource::{DriveItem, DriveItemField},
    ItemId, ItemLocation, OneDrive,
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
    cache: SyncMutex<Cache>,
}

#[derive(Debug)]
struct Cache {
    lru: LruCache<ItemId, DirContent>,
    /// Item -> (parent_id, index_of_parent_entries)
    parent_map: ParentMap,
}

type ParentMap = HashMap<ItemId, (ItemId, usize)>;

impl Cache {
    fn new(capacity: usize) -> Self {
        Self {
            lru: LruCache::new(capacity),
            parent_map: HashMap::new(),
        }
    }

    fn insert(&mut self, item_id: ItemId, dir: DirContent) -> Option<DirContent> {
        // Remove previous links first if exists.
        self.remove(&item_id);

        // Pop LRU.
        if self.lru.len() == self.lru.capacity() {
            let (_, lru_dir) = self.lru.remove_lru().unwrap();
            for ent in &lru_dir.entries {
                assert!(self.parent_map.remove(&ent.item_id).is_some());
            }
        }

        self.parent_map.reserve(dir.entries.len());
        for (idx, ent) in dir.entries.iter().enumerate() {
            assert!(self
                .parent_map
                .insert(ent.item_id.clone(), (item_id.clone(), idx))
                .is_none());
        }

        self.lru.insert(item_id, dir)
    }

    fn remove(&mut self, item_id: &ItemId) -> Option<DirContent> {
        let dir = self.lru.remove(item_id)?;
        for ent in &dir.entries {
            self.parent_map.remove(&ent.item_id);
        }
        Some(dir)
    }

    fn clear(&mut self) {
        self.lru.clear();
        self.parent_map.clear();
    }
}

#[derive(Debug)]
struct DirContent {
    state: DirState,
    entries: Vec<DirEntry>,
    /// name -> index of `entries`
    name_map: HashMap<String, usize>,
}

#[derive(Debug, PartialEq, Eq)]
enum DirState {
    Full,
    Partial,
}

impl DirContent {
    fn new(state: DirState, iter: impl IntoIterator<Item = DirEntry>) -> Self {
        let entries = iter.into_iter().collect::<Vec<_>>();
        let name_map = entries
            .iter()
            .enumerate()
            .map(|(idx, ent)| (ent.name.clone(), idx))
            .collect();
        Self {
            state,
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

    fn insert(&mut self, entry: DirEntry, parent_id: ItemId, parent_map: &mut ParentMap) {
        let new_idx = self.entries.len();
        assert!(self.name_map.insert(entry.name.clone(), new_idx).is_none());
        assert!(parent_map
            .insert(entry.item_id.clone(), (parent_id, new_idx))
            .is_none());
        self.entries.push(entry);
    }

    fn remove_idx(&mut self, idx: usize, parent_map: &mut ParentMap) {
        if idx != self.entries.len() - 1 {
            // A swap-remove will re-locate the last element to the removed one.
            let last = self.entries.last().unwrap();
            *self.name_map.get_mut(&last.name).unwrap() = idx;
            parent_map.get_mut(&last.item_id).unwrap().1 = idx;
        }
        let ent = self.entries.swap_remove(idx);
        assert!(self.name_map.remove(&ent.name).is_some());
        assert!(parent_map.remove(&ent.item_id).is_some());
    }
}

impl DirPool {
    pub fn new(config: Config) -> Self {
        Self {
            cache: SyncMutex::new(Cache::new(config.lru_cache_size)),
        }
    }

    pub async fn read(
        &self,
        parent_id: &ItemId,
        offset: u64,
        count: usize,
        onedrive: &OneDrive,
    ) -> Result<impl AsRef<[DirEntry]>> {
        if let Some(dir) = self.cache.lock().unwrap().lru.get_mut(parent_id) {
            if dir.state == DirState::Full {
                return Ok(dir.read(offset, count));
            }
        }

        // FIXME: Incremental fetching.
        let children_fields = inode::InodeAttr::ATTR_SELECT_FIELDS
            .iter()
            .chain(&[DriveItemField::id, DriveItemField::name])
            .map(|field| field.raw_name())
            .collect::<Vec<_>>();
        let opt = ObjectOption::new()
            .select(&[
                // `id` is required, or we'll get 400 Bad Request.
                DriveItemField::id,
                DriveItemField::children,
            ])
            .expand(DriveItemField::children, Some(&children_fields));
        let dir_item = onedrive
            .get_item_with_option(ItemLocation::from_id(parent_id), opt)
            .await?
            .expect("No If-None-Match");

        let dir = DirContent::new(
            DirState::Full,
            dir_item.children.unwrap().into_iter().map(|item| {
                let attr = inode::InodeAttr::parse_item(&item).expect("Invalid DriveItem");
                let name = item.name.unwrap();
                let item_id = item.id.unwrap();
                DirEntry {
                    item_id,
                    name,
                    attr,
                }
            }),
        );

        let ret = dir.read(offset, count);

        self.cache.lock().unwrap().insert(parent_id.clone(), dir);

        Ok(ret)
    }

    /// Query InodeAttr by item id from cache.
    pub fn get_item_attr_cache(&self, item_id: &ItemId) -> Option<inode::InodeAttr> {
        let mut cache = self.cache.lock().unwrap();
        let cache = &mut *cache;
        let (parent_id, idx) = cache.parent_map.get(&item_id)?;
        Some(
            cache.lru.get_mut(parent_id).unwrap().entries[*idx]
                .attr
                .clone(),
        )
    }

    /// Lookup name of a directory in cache and return ItemId.
    ///
    /// `None` for cache miss.
    /// `Some(None) for not found.
    /// `Some(Some(_))` for found.
    pub fn lookup_cache(&self, parent_id: &ItemId, name: &str) -> Option<Option<DirEntry>> {
        let mut cache = self.cache.lock().unwrap();
        let dir = cache.lru.get_mut(parent_id)?;
        match dir.lookup(name).cloned() {
            Some(ent) => Some(Some(ent)),
            None if dir.state == DirState::Full => Some(None),
            None => None,
        }
    }

    /// Cache a child of a directory if not cached.
    pub fn touch(&self, parent_id: ItemId, entry: DirEntry) {
        let mut cache = self.cache.lock().unwrap();
        let cache = &mut *cache;
        match cache.lru.get_mut(&parent_id) {
            Some(dir) => match dir.state {
                DirState::Full => {}
                DirState::Partial => {
                    // Only insert if not already cached.
                    if cache.parent_map.get_mut(&entry.item_id).is_none() {
                        dir.insert(entry, parent_id, &mut cache.parent_map);
                    }
                }
            },
            None => {
                let dir = DirContent::new(DirState::Partial, std::iter::once(entry));
                cache.insert(parent_id, dir);
            }
        }
    }

    /// Clear all cache.
    pub fn clear_cache(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Sync item changes from remote. Items not in cache are skipped.
    pub fn sync_items(&self, items: &[DriveItem]) {
        let mut cache = self.cache.lock().unwrap();
        // Manually deref the guard, or we cannot mutably borrow two different fields.
        let cache = &mut *cache;

        for item in items {
            let item_id = item.id.as_ref().unwrap();

            // 1. Remove the old item from its parent directory in cache.
            if let Some(&(ref old_parent_id, old_idx)) = cache.parent_map.get(item_id) {
                log::debug!(
                    "Remove {:?} from cached directory {:?}",
                    item_id,
                    old_parent_id,
                );

                cache
                    .lru
                    .get_mut(old_parent_id)
                    .unwrap()
                    .remove_idx(old_idx, &mut cache.parent_map);
            }

            // For delete event, we are done here.
            if item.deleted.is_some() {
                continue;
            }

            let item_name = item.name.as_ref().unwrap();
            let item_attr = inode::InodeAttr::parse_item(&item).unwrap();
            let cur_parent_id = match inode::InodeAttr::parse_parent_id(item) {
                Some(id) => id,
                None => {
                    // Only root directory itself has no parent.
                    assert!(item.root.is_some());
                    continue;
                }
            };

            // 2. Insert the new item if the new parent directory is already in cache.
            if let Some(new_dir) = cache.lru.get_mut(&cur_parent_id) {
                log::debug!(
                    "Add {:?} into cached directory {:?}",
                    item_id,
                    cur_parent_id,
                );

                new_dir.insert(
                    DirEntry {
                        name: item_name.clone(),
                        item_id: item_id.clone(),
                        attr: item_attr.clone(),
                    },
                    cur_parent_id.clone(),
                    &mut cache.parent_map,
                );
            }
        }
    }
}
