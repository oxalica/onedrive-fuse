use fuse::*;
use http::StatusCode;
use libc::c_int;
use log::*;
use onedrive_api::{option::ObjectOption, resource, FileName, ItemId, ItemLocation, OneDrive};
use std::{
    collections::hash_map::{Entry, HashMap},
    ffi::OsStr,
    sync::Arc,
};
use time::Timespec;
use tokio::sync::Mutex;

const GENERATION: u64 = 0;
const BLOCK_SIZE: u32 = 512;
const FRAGMENT_SIZE: u32 = 512;
const NAME_LEN: u32 = 2048;

pub struct Filesystem {
    inner: Arc<FilesystemInner>,
}

struct FilesystemInner {
    onedrive: OneDrive,
    uid: u32,
    gid: u32,
    ino_id_map: Mutex<InoIdMap>,
}

#[derive(Default)]
struct InoIdMap {
    map: HashMap<u64, InoIdMapData>,
    rev_map: HashMap<ItemId, u64>,
}

struct InoIdMapData {
    refs: u64,
    item_id: ItemId,
}

impl Filesystem {
    pub fn new(onedrive: OneDrive, uid: u32, gid: u32) -> Self {
        Self {
            inner: Arc::new(FilesystemInner {
                onedrive,
                uid,
                gid,
                ino_id_map: Default::default(),
            }),
        }
    }

    fn spawn<F, Fut>(&self, f: F)
    where
        F: FnOnce(Arc<FilesystemInner>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn(f(inner));
    }
}

impl FilesystemInner {
    async fn get_item_id(&self, ino: u64) -> Option<ItemId> {
        self.ino_id_map
            .lock()
            .await
            .map
            .get(&ino)
            .map(|v| v.item_id.clone())
    }

    async fn get_or_alloc_ino(&self, item_id: ItemId) -> u64 {
        let g = &mut *self.ino_id_map.lock().await;
        debug_assert_eq!(g.map.len(), g.rev_map.len());
        match g.rev_map.entry(item_id.clone()) {
            Entry::Occupied(ent) => {
                let ino = *ent.get();
                g.map.get_mut(&ino).unwrap().refs += 1;
                ino
            }
            Entry::Vacant(ent) => {
                let ino = g.map.len() as u64 + 2; // Starts at 2.
                debug!("alloc new ino #{} -> {}", ino, item_id.as_str());
                ent.insert(ino);
                g.map.insert(ino, InoIdMapData { item_id, refs: 1 });
                ino
            }
        }
    }

    async fn free_ino(&self, ino: u64, refs: u64) {
        let mut g = self.ino_id_map.lock().await;
        debug_assert_eq!(g.map.len(), g.rev_map.len());
        {
            let val = g.map.get_mut(&ino).unwrap();
            if refs < val.refs {
                val.refs -= refs;
                return;
            }
        }
        // Free
        let item_id = g.map.remove(&ino).unwrap().item_id;
        debug!("free ino #{} -> {}", ino, item_id.as_str());
        g.rev_map.remove(&item_id).unwrap();
    }

    fn item_attr_option(&self) -> ObjectOption<resource::DriveItemField> {
        use resource::DriveItemField;
        ObjectOption::new().select(&[
            DriveItemField::folder,
            DriveItemField::size,
            DriveItemField::created_date_time,
            DriveItemField::last_modified_date_time,
        ])
    }

    fn parse_item_attr(&self, ino: u64, item: &resource::DriveItem) -> FileAttr {
        let kind = match &item.folder {
            Some(_) => FileType::Directory,
            None => FileType::RegularFile,
        };
        let size = item.size.unwrap() as u64;

        let crtime = cvt_time(item.created_date_time.as_deref().unwrap());
        let mtime = cvt_time(item.last_modified_date_time.as_deref().unwrap());
        let ctime = mtime;
        let atime = mtime;

        FileAttr {
            ino,
            size,
            blocks: to_blocks_ceil(size),
            atime,
            mtime,
            ctime,
            crtime,
            kind,
            perm: 0o755,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
        }
    }
}

impl fuse::Filesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        info!("initialize");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        info!("destroy");
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        use onedrive_api::resource::DriveField;

        self.spawn(|inner| async move {
            let drive = match inner
                .onedrive
                .get_drive_with_option(ObjectOption::new().select(&[DriveField::quota]))
                .await
            {
                Ok(drive) => drive,
                Err(err) => {
                    error!("statfs: {}", err);
                    return reply.error(libc::EIO);
                }
            };

            #[derive(Debug, serde::Deserialize)]
            struct Quota {
                total: u64,
                remaining: u64,
                // used: u64,
            }

            let quota: Quota = match serde_json::from_value(*drive.quota.unwrap()) {
                Ok(quota) => quota,
                Err(err) => {
                    error!("statfs: {}", err);
                    return reply.error(libc::EIO);
                }
            };

            reply.statfs(
                to_blocks_ceil(quota.total),
                to_blocks_floor(quota.remaining),
                to_blocks_floor(quota.remaining),
                0,
                0,
                BLOCK_SIZE,
                NAME_LEN,
                FRAGMENT_SIZE,
            )
        });
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup #{}/{}", parent, name.to_string_lossy());

        let name = name.to_owned();
        self.spawn(|inner| async move {
            use resource::DriveItemField;

            let path;
            let parent_item_id;
            let loc = match cvt_name(&name) {
                None => return reply.error(libc::ENOENT),
                Some(name) if parent == FUSE_ROOT_ID => {
                    path = format!("/{}", name.as_str());
                    ItemLocation::from_path(&path).unwrap()
                }
                Some(name) => {
                    parent_item_id = inner.get_item_id(parent).await.unwrap();
                    ItemLocation::child_of_id(&parent_item_id, name)
                }
            };

            let item = match inner
                .onedrive
                .get_item_with_option(loc, inner.item_attr_option().select(&[DriveItemField::id]))
                .await
            {
                Ok(item) => item.unwrap(),
                Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                    return reply.error(libc::ENOENT);
                }
                Err(err) => {
                    error!("lookup: {}", err);
                    return reply.error(libc::EIO);
                }
            };

            let mut attr = inner.parse_item_attr(0, &item);
            attr.ino = inner.get_or_alloc_ino(item.id.unwrap()).await;
            let ttl = Timespec::new(0, 0);
            reply.entry(&ttl, &attr, GENERATION);
        });
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        debug!("forget #{}", ino);
        self.spawn(move |inner| async move {
            inner.free_ino(ino, nlookup).await;
        })
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr #{}", ino);
        self.spawn(move |inner| async move {
            let item_id;
            let loc = if ino == FUSE_ROOT_ID {
                ItemLocation::root()
            } else {
                item_id = inner.get_item_id(ino).await.unwrap();
                ItemLocation::from_id(&item_id)
            };

            let item = match inner
                .onedrive
                .get_item_with_option(loc, inner.item_attr_option())
                .await
            {
                Ok(item) => item.unwrap(),
                Err(err) => {
                    error!("getattr: {}", err);
                    return reply.error(libc::EIO);
                }
            };

            let attr = inner.parse_item_attr(ino, &item);
            let ttl = Timespec::new(0, 0);
            reply.attr(&ttl, &attr);
        });
    }
}

fn cvt_name(s: &OsStr) -> Option<&FileName> {
    FileName::new(s.to_str()?)
}

fn cvt_time(time: &str) -> Timespec {
    // FIXME
    time::strptime(time, "%Y-%m-%dT%H:%M:%S.%f%z")
        .or_else(|_| time::strptime(time, "%Y-%m-%dT%H:%M:%S%z"))
        .unwrap_or_else(|err| panic!("Invalid time '{}': {}", time, err))
        .to_timespec()
}

fn to_blocks_ceil(sz: u64) -> u64 {
    (sz + u64::from(BLOCK_SIZE)) / u64::from(BLOCK_SIZE)
}

fn to_blocks_floor(sz: u64) -> u64 {
    sz / u64::from(BLOCK_SIZE)
}
