use crate::{error::Result, vfs::inode::InodePool};
use onedrive_api::{ItemId, ItemLocation, OneDrive};
use serde::Deserialize;
use sharded_slab::{Clear, Pool};
use std::{convert::TryFrom, ffi::OsString, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub name: OsString,
    pub is_directory: bool,
}

pub struct DirPool {
    config: Config,
    /// FIXME: Remove `Arc`. https://github.com/hawkw/sharded-slab/issues/43
    pool: Pool<Dir>,
}

#[derive(Deserialize)]
pub struct Config {
    fetch_page_size: std::num::NonZeroUsize,
}

struct Dir {
    item_id: ItemId,
    entries: Arc<Mutex<Option<Vec<DirEntry>>>>,
}

// Required by `Pool`.
impl Default for Dir {
    fn default() -> Self {
        Self {
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

    pub async fn alloc(&self, item_id: ItemId) -> u64 {
        let key = self
            .pool
            .create(|p| p.item_id = item_id)
            .expect("Pool is full");
        Self::key_to_fh(key)
    }

    pub async fn free(&self, fh: u64) -> Option<()> {
        if self.pool.clear(Self::fh_to_key(fh)) {
            Some(())
        } else {
            None
        }
    }

    pub async fn read<D: Default + Clear>(
        &self,
        fh: u64,
        offset: u64,
        inode_pool: &InodePool<D>,
        onedrive: &OneDrive,
    ) -> Result<impl AsRef<[DirEntry]>> {
        use onedrive_api::{option::CollectionOption, resource::DriveItemField};

        let offset = usize::try_from(offset).unwrap();
        let key = Self::fh_to_key(fh);

        let entries = self.pool.get(key).expect("Invalid fh").entries.clone();
        let mut entries = entries.lock().await;
        if entries.is_none() {
            let item_id = self.pool.get(key).expect("Invalid fh").item_id.clone();
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
                let ino = inode_pool.get_or_alloc_ino(item_id).await;
                // TODO: Cache inode attrs.
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
