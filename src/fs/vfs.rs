use super::CResult;
use anyhow::Result as AResult;
use fuse::FUSE_ROOT_ID;
use onedrive_api::{FileName, ItemId, ItemLocation, OneDrive};
use reqwest::StatusCode;
use sharded_slab::{Clear, Pool};
use std::{
    collections::hash_map::{Entry, HashMap},
    ffi::OsStr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use time::Timespec;
use tokio::sync::Mutex;

trait ResultExt<T> {
    fn c_err(self, target: &'static str, errno: libc::c_int) -> CResult<T>;
}

impl<T, E: std::fmt::Display> ResultExt<T> for Result<T, E> {
    fn c_err(self, target: &'static str, errno: libc::c_int) -> CResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => {
                log::info!(target: target, "{}", err);
                Err(errno)
            }
        }
    }
}

#[derive(Clone)]
pub struct Statfs {
    pub total: u64,
    pub free: u64,
}

enum OwnedItemLocation {
    Root,
    Id(ItemId),
    Path(String),
    ChildOfId(ItemId, String),
}

impl<'a> Into<ItemLocation<'a>> for &'a OwnedItemLocation {
    fn into(self) -> ItemLocation<'a> {
        use OwnedItemLocation::*;
        match self {
            Root => ItemLocation::root(),
            Id(id) => ItemLocation::from_id(id),
            Path(path) => ItemLocation::from_path(path).unwrap(),
            ChildOfId(id, name) => ItemLocation::child_of_id(id, FileName::new(name).unwrap()),
        }
    }
}

#[derive(Default)]
pub struct Vfs {
    inode_pool: InodePool,
}

impl Vfs {
    pub async fn statfs(&self, onedrive: &OneDrive) -> CResult<Statfs> {
        // TODO: Cache
        self.statfs_raw(onedrive).await.c_err("statfs", libc::EIO)
    }

    async fn statfs_raw(&self, onedrive: &OneDrive) -> AResult<Statfs> {
        use onedrive_api::{option::ObjectOption, resource::DriveField};

        #[derive(Debug, serde::Deserialize)]
        struct Quota {
            total: u64,
            remaining: u64,
            // used: u64,
        }

        let drive = onedrive
            .get_drive_with_option(ObjectOption::new().select(&[DriveField::quota]))
            .await?;
        let quota: Quota = serde_json::from_value(*drive.quota.unwrap())?;
        Ok(Statfs {
            total: quota.total,
            free: quota.remaining,
        })
    }

    fn cvt_filename<'a>(&self, name: &'a OsStr) -> Option<&'a FileName> {
        name.to_str().and_then(FileName::new)
    }

    fn location_of_ino_child(&self, parent: u64, child: &OsStr) -> Option<OwnedItemLocation> {
        let child = self.cvt_filename(child)?;
        if parent == FUSE_ROOT_ID {
            Some(OwnedItemLocation::Path(format!("/{}", child.as_str())))
        } else {
            Some(OwnedItemLocation::ChildOfId(
                self.inode_pool.get_item_id(parent)?,
                child.as_str().to_owned(),
            ))
        }
    }

    fn location_of_ino(&self, ino: u64) -> Option<OwnedItemLocation> {
        if ino == FUSE_ROOT_ID {
            Some(OwnedItemLocation::Root)
        } else {
            Some(OwnedItemLocation::Id(self.inode_pool.get_item_id(ino)?))
        }
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
        onedrive: &OneDrive,
    ) -> CResult<(u64, InodeAttr, Duration)> {
        // Check from directory cache.
        let loc = self
            .location_of_ino_child(parent_ino, child_name)
            .ok_or(libc::EINVAL)?;
        let (item_id, attr) = self
            .get_attr_raw((&loc).into(), onedrive)
            .await
            .c_err("lookup", libc::EIO)?
            // Expire the cache.
            .ok_or(libc::ENOENT)?;
        let ino = self.inode_pool.get_or_alloc_ino(item_id).await;
        let ttl = Duration::new(0, 0);
        Ok((ino, attr, ttl))
    }

    pub async fn forget(&self, ino: u64, count: u64) -> CResult<()> {
        self.inode_pool.free(ino, count).await.ok_or(libc::EINVAL)
    }

    pub async fn get_attr(&self, ino: u64, onedrive: &OneDrive) -> CResult<(InodeAttr, Duration)> {
        // TODO: Attr cache
        let loc = self.location_of_ino(ino).ok_or(libc::EINVAL)?;
        let (_, attr) = self
            .get_attr_raw((&loc).into(), onedrive)
            .await
            .c_err("get_attr", libc::EIO)?
            .ok_or(libc::ENOENT)?;
        let ttl = Duration::new(0, 0);
        Ok((attr, ttl))
    }

    async fn get_attr_raw(
        &self,
        loc: ItemLocation<'_>,
        onedrive: &OneDrive,
    ) -> AResult<Option<(ItemId, InodeAttr)>> {
        use onedrive_api::{option::ObjectOption, resource::DriveItemField};

        fn parse_time(s: &str) -> Timespec {
            // FIXME
            time::strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z")
                .or_else(|_| time::strptime(s, "%Y-%m-%dT%H:%M:%S%z"))
                .unwrap_or_else(|err| panic!("Invalid time '{}': {}", s, err))
                .to_timespec()
        }

        // TODO: If-None-Match
        let item = match onedrive
            .get_item_with_option(
                loc,
                ObjectOption::new().select(&[
                    DriveItemField::id,
                    DriveItemField::size,
                    DriveItemField::last_modified_date_time,
                    DriveItemField::created_date_time,
                    DriveItemField::folder,
                ]),
            )
            .await
        {
            Ok(Some(item)) => item,
            Ok(None) => unreachable!("No If-None-Match"),
            Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };

        let item_id = item.id.unwrap();
        let attr = InodeAttr {
            size: item.size.unwrap() as u64,
            mtime: parse_time(item.last_modified_date_time.as_deref().unwrap()),
            crtime: parse_time(item.created_date_time.as_deref().unwrap()),
            is_directory: item.folder.is_some(),
        };
        Ok(Some((item_id, attr)))
    }
}

// This should not hold any heap-allocation due to the requirement `Inode: Clear`.
pub struct InodeAttr {
    pub size: u64,
    pub mtime: Timespec,
    pub crtime: Timespec,
    pub is_directory: bool,
}

#[derive(Default)]
struct InodePool {
    pool: Pool<Inode>,
    rev_map: Mutex<HashMap<ItemId, usize>>,
}

struct Inode {
    ref_count: AtomicU64,
    item_id: ItemId,
}

impl Clear for Inode {
    fn clear(&mut self) {
        self.item_id.0.clear();
    }
}

// Required by `Pool`. Set to an invalid state.
impl Default for Inode {
    fn default() -> Self {
        Self {
            ref_count: 0.into(),
            item_id: ItemId(String::new()),
        }
    }
}

impl Inode {
    fn new(item_id: ItemId) -> Self {
        Self {
            ref_count: 1.into(),
            item_id,
        }
    }
}

impl InodePool {
    fn idx_to_ino(idx: usize) -> u64 {
        (idx as u64).wrapping_add(FUSE_ROOT_ID + 1)
    }

    fn ino_to_idx(ino: u64) -> usize {
        assert_ne!(ino, FUSE_ROOT_ID);
        ino.wrapping_sub(FUSE_ROOT_ID + 1) as usize
    }

    async fn get_or_alloc_ino(&self, item_id: ItemId) -> u64 {
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

    fn get_item_id(&self, ino: u64) -> Option<ItemId> {
        Some(self.pool.get(Self::ino_to_idx(ino))?.item_id.clone())
    }

    async fn free(&self, ino: u64, count: u64) -> Option<()> {
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
}
