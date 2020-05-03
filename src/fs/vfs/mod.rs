use crate::fs::CResult;
use anyhow::Result as AResult;
use fuse::FUSE_ROOT_ID;
use onedrive_api::{FileName, ItemId, ItemLocation, OneDrive};
use reqwest::StatusCode;
use std::{ffi::OsStr, time::Duration};
use time::Timespec;

mod dir;
mod inode;
pub use dir::DirEntry;
pub use inode::InodeAttr;

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
    inode_pool: inode::InodePool,
    dir_pool: dir::DirPool,
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

    pub async fn open_dir(&self, ino: u64) -> CResult<u64> {
        if ino == FUSE_ROOT_ID {
            Ok(self.dir_pool.alloc(None).await)
        } else {
            let item_id = self.inode_pool.get_item_id(ino).ok_or(libc::EINVAL)?;
            Ok(self.dir_pool.alloc(Some(item_id)).await)
        }
    }

    pub async fn close_dir(&self, _ino: u64, fh: u64) -> CResult<()> {
        self.dir_pool.free(fh).await.ok_or(libc::EINVAL)
    }

    pub async fn read_dir(
        &self,
        _ino: u64,
        fh: u64,
        offset: u64,
        onedrive: &OneDrive,
    ) -> CResult<impl AsRef<[DirEntry]>> {
        self.dir_pool
            .read(fh, offset, &self.inode_pool, onedrive)
            .await
    }
}
