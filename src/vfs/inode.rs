use onedrive_api::ItemId;
use sharded_slab::{Clear, Pool};
use std::{
    collections::hash_map::{Entry, HashMap},
    convert::TryFrom as _,
    sync::atomic::{AtomicU64, Ordering},
};
use time::Timespec;
use tokio::sync::Mutex;

// This should not hold any heap-allocation due to the requirement `Inode: Clear`.
#[derive(Clone, Copy)]
pub struct InodeAttr {
    pub size: u64,
    pub mtime: Timespec,
    pub crtime: Timespec,
    pub is_directory: bool,
}

#[derive(Default)]
pub struct InodePool<Data: Default + Clear> {
    /// ino_shift = ino - key
    ino_shift: u64,
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

const ROOT_INO: u64 = fuse::FUSE_ROOT_ID;
static_assertions::const_assert_eq!(ROOT_INO, 1);

impl<Data: Default + Clear> InodePool<Data> {
    /// Initialize inode pool with root id to make operation on root nothing special.
    pub async fn new(root_item_id: ItemId) -> Self {
        let mut ret = Self {
            ino_shift: 0,
            pool: Default::default(),
            rev_map: Default::default(),
        };
        // Root has ref-count initialized at 1.
        let root_key = ret.get_or_alloc_ino(root_item_id).await;
        ret.ino_shift = ROOT_INO - root_key;
        ret
    }

    fn key_to_ino(&self, key: usize) -> u64 {
        u64::try_from(key).unwrap().wrapping_add(self.ino_shift)
    }

    fn ino_to_key(&self, ino: u64) -> usize {
        usize::try_from(ino.wrapping_sub(self.ino_shift)).unwrap()
    }

    pub async fn get_or_alloc_ino(&self, item_id: ItemId) -> u64 {
        let key = match self.rev_map.lock().await.entry(item_id) {
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
        };
        self.key_to_ino(key)
    }

    pub fn get_item_id(&self, ino: u64) -> Option<ItemId> {
        Some(self.pool.get(self.ino_to_key(ino))?.item_id.clone())
    }

    pub async fn free(&self, ino: u64, count: u64) -> Option<()> {
        let key = self.ino_to_key(ino);
        // Lock first to avoid race with get_or_alloc.
        let mut rev_g = self.rev_map.lock().await;
        let g = self.pool.get(key)?;
        let orig_ref_count = g.ref_count.fetch_sub(count, Ordering::Relaxed);
        if count < orig_ref_count {
            return Some(());
        }
        assert!(rev_g.remove(&g.item_id).is_some());
        assert!(self.pool.clear(key));
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

        self.pool.get(self.ino_to_key(ino)).map(Wrap)
    }
}
