use fuse::FUSE_ROOT_ID;
use onedrive_api::ItemId;
use sharded_slab::{Clear, Pool};
use std::{
    collections::hash_map::{Entry, HashMap},
    sync::atomic::{AtomicU64, Ordering},
};
use time::Timespec;
use tokio::sync::Mutex;

// This should not hold any heap-allocation due to the requirement `Inode: Clear`.
pub struct InodeAttr {
    pub size: u64,
    pub mtime: Timespec,
    pub crtime: Timespec,
    pub is_directory: bool,
}

#[derive(Default)]
pub struct InodePool<Data: Default + Clear> {
    pool: Pool<Inode<Data>>,
    rev_map: Mutex<HashMap<ItemId, usize>>,
}

struct Inode<Data> {
    ref_count: AtomicU64,
    item_id: ItemId,
    data: Data,
}

impl<Data: Clear> Clear for Inode<Data> {
    fn clear(&mut self) {
        self.item_id.0.clear();
        self.data.clear();
    }
}

// Required by `Pool`. Set to an invalid state.
impl<Data: Default> Default for Inode<Data> {
    fn default() -> Self {
        Self {
            ref_count: 0.into(),
            item_id: ItemId(String::new()),
            data: Default::default(),
        }
    }
}

impl<Data: Default> Inode<Data> {
    fn new(item_id: ItemId) -> Self {
        Self {
            ref_count: 1.into(),
            item_id,
            data: Default::default(),
        }
    }
}

impl<Data: Default + Clear> InodePool<Data> {
    fn idx_to_ino(idx: usize) -> u64 {
        (idx as u64).wrapping_add(FUSE_ROOT_ID + 1)
    }

    fn ino_to_idx(ino: u64) -> usize {
        assert_ne!(ino, FUSE_ROOT_ID);
        ino.wrapping_sub(FUSE_ROOT_ID + 1) as usize
    }

    pub async fn get_or_alloc_ino(&self, item_id: ItemId) -> u64 {
        let idx = match self.rev_map.lock().await.entry(item_id) {
            Entry::Occupied(ent) => {
                let idx = *ent.get();
                self.pool
                    .get(idx)
                    .unwrap()
                    .ref_count
                    .fetch_add(1, Ordering::Relaxed);
                idx
            }
            Entry::Vacant(ent) => {
                let idx = self
                    .pool
                    .create(|p| *p = Inode::new(ent.key().clone()))
                    .expect("Pool is full");
                ent.insert(idx);
                idx
            }
        };
        Self::idx_to_ino(idx)
    }

    pub fn get_item_id(&self, ino: u64) -> Option<ItemId> {
        Some(self.pool.get(Self::ino_to_idx(ino))?.item_id.clone())
    }

    pub async fn free(&self, ino: u64, count: u64) -> Option<()> {
        let idx = Self::ino_to_idx(ino);
        // Lock first to avoid race with get_or_alloc.
        let mut rev_g = self.rev_map.lock().await;
        let g = self.pool.get(idx)?;
        let orig_ref_count = g.ref_count.fetch_sub(count, Ordering::Relaxed);
        if count < orig_ref_count {
            return Some(());
        }
        assert!(rev_g.remove(&g.item_id).is_some());
        assert!(self.pool.clear(idx));
        Some(())
    }

    pub fn get_data<'a>(&'a self, ino: u64) -> Option<impl std::ops::Deref<Target = Data> + 'a> {
        use sharded_slab::{Config, PoolGuard};

        struct Wrap<'a, Data: Default + Clear, C: Config>(PoolGuard<'a, Inode<Data>, C>);

        impl<Data: Default + Clear, C: Config> std::ops::Deref for Wrap<'_, Data, C> {
            type Target = Data;
            fn deref(&self) -> &Self::Target {
                &self.0.data
            }
        }

        self.pool.get(Self::ino_to_idx(ino)).map(Wrap)
    }
}
