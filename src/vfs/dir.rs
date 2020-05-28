use crate::{
    error::{Error, Result},
    util::de_duration_sec,
    vfs::inode::InodePool,
};
use onedrive_api::{ItemId, ItemLocation, OneDrive};
use serde::Deserialize;
use sharded_slab::{Clear, Pool};
use std::{
    convert::TryFrom,
    ffi::OsString,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as SyncMutex,
    },
    time::Duration,
};
use tokio::{sync::Mutex, task::spawn, time::delay_for};

#[derive(Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub name: OsString,
    pub is_directory: bool,
}

pub struct DirPool {
    config: Config,
    pool: Arc<Pool<Dir>>,
}

#[derive(Deserialize)]
pub struct Config {
    fetch_page_size: std::num::NonZeroUsize,
    #[serde(deserialize_with = "de_duration_sec")]
    entries_cache_ttl: Duration,
}

struct Dir {
    ref_count: AtomicU64,
    item_id: ItemId,
    /// FIXME: Remove `Arc`. https://github.com/hawkw/sharded-slab/issues/43
    entries: Arc<Mutex<Option<Vec<DirEntry>>>>,
}

// Required by `Pool`.
impl Default for Dir {
    fn default() -> Self {
        Self {
            ref_count: 0.into(),
            item_id: ItemId(String::new()),
            entries: Default::default(),
        }
    }
}

impl Clear for Dir {
    fn clear(&mut self) {
        self.item_id.0.clear();
        // Avoid pollution.
        self.entries = Default::default();
    }
}

#[derive(Default, Clone)]
pub struct Cache {
    last_fh: Arc<SyncMutex<Option<u64>>>,
}

impl DirPool {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            pool: Default::default(),
        }
    }

    fn key_to_fh(key: usize) -> u64 {
        u64::try_from(key).unwrap()
    }

    fn fh_to_key(fh: u64) -> usize {
        usize::try_from(fh).unwrap()
    }

    fn alloc(&self, item_id: ItemId, ref_count: u64) -> u64 {
        assert_ne!(ref_count, 0);
        let key = self
            .pool
            .create(|p| {
                p.ref_count = ref_count.into();
                p.item_id = item_id;
            })
            .expect("Pool is full");
        let fh = Self::key_to_fh(key);
        log::debug!("alloc dir fh={}", fh);
        fh
    }

    /// Increase the ref-count of existing `fh`.
    /// `fh` must has at lease one reference during the calling.
    fn alloc_exist(&self, fh: u64) -> Option<()> {
        let prev_ref_count = self
            .pool
            .get(Self::fh_to_key(fh))?
            .ref_count
            // FIXME: Ordering.
            .fetch_add(1, Ordering::SeqCst);
        // We can only
        assert_ne!(prev_ref_count, 0);
        Some(())
    }

    pub fn open(&self, item_id: ItemId, cache: &Cache) -> u64 {
        let ttl = self.config.entries_cache_ttl;
        if ttl == Duration::new(0, 0) {
            return self.alloc(item_id, 1);
        }

        let mut last_fh = cache.last_fh.lock().unwrap();
        if let Some(fh) = *last_fh {
            self.alloc_exist(fh).unwrap();
            return fh;
        }

        // `last_dir_fh` is `None` here.
        log::debug!("open_dir: cache miss");
        // Also referenced by `last_dir_fh`.
        let fh = self.alloc(item_id, 2);
        *last_fh = Some(fh);

        // Set timeout to expire and release the cache.
        let last_fh_arc = cache.last_fh.clone();
        let pool_arc = self.pool.clone();
        spawn(async move {
            delay_for(ttl).await;
            let mut guard = last_fh_arc.lock().unwrap();
            // `last_dir_fh` will not be modified unless it is `None`.
            // So it should be kept during the TTL.
            assert_eq!(*guard, Some(fh));
            *guard = None;
            // Note that this `free` just decreases ref-count. `fh` may still be alive outside.
            // For example, `open_dir` and keep `fh` for more than cache TTL.
            Self::free_inner(&pool_arc, fh).unwrap();
        });

        fh
    }

    pub fn free(&self, fh: u64) -> Option<()> {
        Self::free_inner(&self.pool, fh)
    }

    fn free_inner(pool: &Pool<Dir>, fh: u64) -> Option<()> {
        let key = Self::fh_to_key(fh);
        let prev_count = pool.get(key)?.ref_count.fetch_sub(1, Ordering::Relaxed);
        assert_ne!(prev_count, 0);
        if prev_count == 1 {
            // Since it is the last handle, no one can race us with increasement.
            assert!(pool.clear(key));
            log::debug!("release dir fh={}", fh);
        }
        Some(())
    }

    pub async fn read(
        &self,
        fh: u64,
        offset: u64,
        inode_pool: &InodePool,
        onedrive: &OneDrive,
    ) -> Result<impl AsRef<[DirEntry]>> {
        use onedrive_api::{option::CollectionOption, resource::DriveItemField};

        let offset = usize::try_from(offset).unwrap();
        let key = Self::fh_to_key(fh);

        let entries = self
            .pool
            .get(key)
            .ok_or(Error::InvalidHandle(fh))?
            .entries
            .clone();
        let mut entries = entries.lock().await;
        if entries.is_none() {
            let item_id = self.pool.get(key).unwrap().item_id.clone();
            let fetcher = onedrive
                .list_children_with_option(
                    ItemLocation::from_id(&item_id),
                    CollectionOption::new()
                        .select(&[
                            DriveItemField::id,
                            DriveItemField::name,
                            DriveItemField::folder,
                        ])
                        .page_size(self.config.fetch_page_size.get()),
                )
                .await?
                .expect("No If-Non-Match");

            // TODO: Incremental fetch.
            let items = fetcher.fetch_all(onedrive).await?;

            let mut ret = Vec::with_capacity(items.len());
            for item in items {
                let item_id = item.id.unwrap();
                // TODO: Cache inode attrs.
                let ino = inode_pool.touch(item_id).await;
                ret.push(DirEntry {
                    ino,
                    name: item.name.unwrap().into(),
                    is_directory: item.folder.is_some(),
                });
            }

            *entries = Some(ret);
        }

        // TODO: Avoid copy.
        Ok(entries.as_ref().unwrap()[offset..].to_vec())
    }
}
