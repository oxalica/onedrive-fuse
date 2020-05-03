use crate::{error::CResult, vfs::inode::InodePool, vfs::ResultExt as _};
use onedrive_api::{ItemId, ItemLocation, ListChildrenFetcher, OneDrive};
use reqwest::StatusCode;
use sharded_slab::{Clear, Pool};
use std::{convert::TryFrom, ffi::OsString, sync::Mutex as SyncMutex};

#[derive(Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub name: OsString,
    pub is_directory: bool,
}

#[derive(Default)]
pub struct DirPool {
    // To make it `Clear`.
    pool: Pool<Dir>,
}

// TODO: Incremental fetch.
struct Dir {
    // `None` for root.
    item_id: Option<ItemId>,
    // Will be filled in the first `read`. Directory content will be snapshoted and
    // kept immutable between `opendir` and `releasedir`.
    entries: SyncMutex<Option<Vec<DirEntry>>>,
}

// Required by `Pool`.
impl Default for Dir {
    fn default() -> Self {
        Self {
            item_id: None,
            entries: SyncMutex::new(None),
        }
    }
}

impl Clear for Dir {
    fn clear(&mut self) {
        self.item_id.clear();
        self.entries.clear();
    }
}

impl DirPool {
    fn idx_to_fh(idx: usize) -> u64 {
        u64::try_from(idx).unwrap()
    }

    fn fh_to_idx(fh: u64) -> usize {
        usize::try_from(fh).unwrap()
    }

    // `None` for root
    pub async fn alloc(&self, item_id: Option<ItemId>) -> u64 {
        let idx = self
            .pool
            .create(|p| {
                *p = Dir {
                    item_id,
                    // Lazy fetch.
                    entries: SyncMutex::new(None),
                };
            })
            .expect("Pool is full");
        Self::idx_to_fh(idx)
    }

    pub async fn free(&self, fh: u64) -> Option<()> {
        if self.pool.clear(Self::fh_to_idx(fh)) {
            Some(())
        } else {
            None
        }
    }

    pub async fn read(
        &self,
        fh: u64,
        offset: u64,
        inode_pool: &InodePool,
        onedrive: &OneDrive,
    ) -> CResult<impl AsRef<[DirEntry]>> {
        use onedrive_api::{option::CollectionOption, resource::DriveItemField};

        let offset = usize::try_from(offset).unwrap();

        let item_id;
        let loc = {
            let dir = self.pool.get(Self::fh_to_idx(fh)).ok_or(libc::EINVAL)?;
            if let Some(v) = &*dir.entries.lock().unwrap() {
                // TODO: Avoid copy.
                return Ok(v[offset..].to_vec());
            }
            match &dir.item_id {
                None => ItemLocation::root(),
                Some(id) => {
                    item_id = id.clone();
                    ItemLocation::from_id(&item_id)
                }
            }
        };

        // FIXME: Race request.
        let fetcher: ListChildrenFetcher = match onedrive
            .list_children_with_option(
                loc,
                CollectionOption::new().select(&[
                    DriveItemField::id,
                    DriveItemField::name,
                    DriveItemField::folder,
                ]),
            )
            .await
        {
            Ok(Some(fetcher)) => fetcher,
            Ok(None) => unreachable!("No If-Non-Match"),
            Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                return Err(libc::ENOENT);
            }
            Err(err) => {
                return Err(err).c_err("read_dir", libc::EIO);
            }
        };
        let items = fetcher
            .fetch_all(onedrive)
            .await
            .c_err("read_dir", libc::EIO)?;

        let mut entries = Vec::with_capacity(items.len());
        for item in items {
            let item_id = item.id.unwrap();
            let ino = inode_pool.get_or_alloc_ino(item_id).await;
            entries.push(DirEntry {
                ino,
                name: item.name.unwrap().into(),
                is_directory: item.folder.is_some(),
            });
        }

        let ret = entries[offset..].to_vec();

        {
            let dir = self.pool.get(Self::fh_to_idx(fh)).ok_or(libc::EINVAL)?;
            *dir.entries.lock().unwrap() = Some(entries);
        }

        Ok(ret)
    }
}
