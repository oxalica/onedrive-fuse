//! Inode number pool with bidirectional mapping to `ItemId`.
use crate::vfs::error::{Error, Result};
use onedrive_api::ItemId;
use std::{
    collections::hash_map::{Entry, HashMap},
    sync::Mutex as SyncMutex,
};

pub struct InodeIdPool {
    inner: SyncMutex<PoolInner>,
    root_ino: u64,
}

struct PoolInner {
    inode_counter: u64,
    /// ino -> (reference_count, item_id)
    map: HashMap<u64, (u64, ItemId)>,
    /// item_id -> ino
    rev_map: HashMap<ItemId, u64>,
}

impl InodeIdPool {
    pub fn new(root_ino: u64) -> Self {
        InodeIdPool {
            inner: SyncMutex::new(PoolInner {
                // Do not allocate root inode id automatically.
                inode_counter: root_ino + 1,
                map: HashMap::new(),
                rev_map: HashMap::new(),
            }),
            root_ino,
        }
    }

    /// Set the root item id. This method can only be called once.
    pub fn set_root_item_id(&self, item_id: ItemId) {
        let mut inner = self.inner.lock().unwrap();
        assert!(inner
            .map
            .insert(self.root_ino, (1, item_id.clone()))
            .is_none());
        assert!(inner.rev_map.insert(item_id, self.root_ino).is_none());
    }

    /// Update InodeAttr of existing inode or allocate a new inode,
    /// also increase the reference count.
    pub fn acquire_or_alloc(&self, item_id: &ItemId) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        match inner.rev_map.get(item_id) {
            Some(&ino) => {
                inner.map.get_mut(&ino).unwrap().0 += 1;
                ino
            }
            None => {
                let ino = inner.inode_counter;
                assert_ne!(ino, u64::MAX);
                inner.inode_counter += 1;
                inner.map.insert(ino, (1, item_id.clone()));
                inner.rev_map.insert(item_id.clone(), ino);
                ino
            }
        }
    }

    /// Decrease reference count of an inode by `count`.
    /// Return if it is freed.
    pub fn free(&self, ino: u64, count: u64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        match inner.map.entry(ino) {
            Entry::Vacant(_) => Err(Error::InvalidInode(ino)),
            Entry::Occupied(mut ent) => {
                assert!(count <= ent.get_mut().0);
                if ent.get_mut().0 == count {
                    let (_, item_id) = ent.remove();
                    assert!(inner.rev_map.remove(&item_id).is_some());
                    Ok(true)
                } else {
                    ent.get_mut().0 -= count;
                    Ok(false)
                }
            }
        }
    }

    /// Get item id from an existing inode.
    pub fn get_item_id(&self, ino: u64) -> Result<ItemId> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .map
            .get(&ino)
            .ok_or(Error::InvalidInode(ino))?
            .1
            .clone())
    }
}
