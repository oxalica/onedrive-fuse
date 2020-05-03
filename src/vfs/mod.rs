use crate::error::{Error, Result};
use fuse::FUSE_ROOT_ID;
use onedrive_api::{FileName, ItemId, ItemLocation, OneDrive};
use std::{ffi::OsStr, time::Duration};
use time::Timespec;

mod dir;
mod inode;
pub use dir::DirEntry;
pub use inode::InodeAttr;

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
    pub async fn statfs(&self, onedrive: &OneDrive) -> Result<Statfs> {
        // TODO: Cache
        self.statfs_raw(onedrive).await
    }

    async fn statfs_raw(&self, onedrive: &OneDrive) -> Result<Statfs> {
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
        let quota: Quota =
            serde_json::from_value(*drive.quota.unwrap()).expect("Deserialize error");
        Ok(Statfs {
            total: quota.total,
            free: quota.remaining,
        })
    }

    fn cvt_filename<'a>(&self, name: &'a OsStr) -> Result<&'a FileName> {
        name.to_str()
            .and_then(FileName::new)
            .ok_or(Error::InvalidArgument("Invalid filename"))
    }

    fn location_of_ino_child(&self, parent: u64, child: &OsStr) -> Result<OwnedItemLocation> {
        let child = self.cvt_filename(child)?;
        if parent == FUSE_ROOT_ID {
            Ok(OwnedItemLocation::Path(format!("/{}", child.as_str())))
        } else {
            Ok(OwnedItemLocation::ChildOfId(
                self.inode_pool.get_item_id(parent).expect("Invalid inode"),
                child.as_str().to_owned(),
            ))
        }
    }

    fn location_of_ino(&self, ino: u64) -> OwnedItemLocation {
        if ino == FUSE_ROOT_ID {
            OwnedItemLocation::Root
        } else {
            OwnedItemLocation::Id(self.inode_pool.get_item_id(ino).expect("Invalid inode"))
        }
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
        onedrive: &OneDrive,
    ) -> Result<(u64, InodeAttr, Duration)> {
        // Check from directory cache.
        let loc = self.location_of_ino_child(parent_ino, child_name)?;
        let (item_id, attr) = self.get_attr_raw((&loc).into(), onedrive).await?;
        let ino = self.inode_pool.get_or_alloc_ino(item_id).await;
        let ttl = Duration::new(0, 0);
        Ok((ino, attr, ttl))
    }

    pub async fn forget(&self, ino: u64, count: u64) {
        self.inode_pool
            .free(ino, count)
            .await
            .expect("Invalid inode");
    }

    pub async fn get_attr(&self, ino: u64, onedrive: &OneDrive) -> Result<(InodeAttr, Duration)> {
        // TODO: Attr cache
        let loc = self.location_of_ino(ino);
        let (_, attr) = self.get_attr_raw((&loc).into(), onedrive).await?;
        let ttl = Duration::new(0, 0);
        Ok((attr, ttl))
    }

    async fn get_attr_raw(
        &self,
        loc: ItemLocation<'_>,
        onedrive: &OneDrive,
    ) -> Result<(ItemId, InodeAttr)> {
        use onedrive_api::{option::ObjectOption, resource::DriveItemField};

        fn parse_time(s: &str) -> Timespec {
            // FIXME
            time::strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z")
                .or_else(|_| time::strptime(s, "%Y-%m-%dT%H:%M:%S%z"))
                .unwrap_or_else(|err| panic!("Invalid time '{}': {}", s, err))
                .to_timespec()
        }

        // TODO: If-None-Match
        let item = onedrive
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
            .await?
            .expect("No If-None-Match");

        let item_id = item.id.unwrap();
        let attr = InodeAttr {
            size: item.size.unwrap() as u64,
            mtime: parse_time(item.last_modified_date_time.as_deref().unwrap()),
            crtime: parse_time(item.created_date_time.as_deref().unwrap()),
            is_directory: item.folder.is_some(),
        };
        Ok((item_id, attr))
    }

    pub async fn open_dir(&self, ino: u64) -> Result<u64> {
        if ino == FUSE_ROOT_ID {
            Ok(self.dir_pool.alloc(None).await)
        } else {
            let item_id = self.inode_pool.get_item_id(ino).expect("Invalid inode");
            Ok(self.dir_pool.alloc(Some(item_id)).await)
        }
    }

    pub async fn close_dir(&self, _ino: u64, fh: u64) {
        self.dir_pool.free(fh).await.expect("Invalid fh");
    }

    pub async fn read_dir(
        &self,
        _ino: u64,
        fh: u64,
        offset: u64,
        onedrive: &OneDrive,
    ) -> Result<impl AsRef<[DirEntry]>> {
        self.dir_pool
            .read(fh, offset, &self.inode_pool, onedrive)
            .await
    }
}
