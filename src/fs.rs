use fuse::*;
use http::StatusCode;
use libc::c_int;
use log::*;
use onedrive_api::{
    option::{CollectionOption, ObjectOption},
    resource, FileName, ItemId, ItemLocation, OneDrive,
};
use std::{
    // FIXME: Concurrent map
    collections::hash_map::{Entry, HashMap},
    ffi::{OsStr, OsString},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use time::Timespec;
use tokio::sync::Mutex;

const GENERATION: u64 = 0;
const BLOCK_SIZE: u32 = 512;
const FRAGMENT_SIZE: u32 = 512;
const NAME_LEN: u32 = 2048;

static_assertions::const_assert_eq!(FUSE_ROOT_ID, 1);

pub struct Filesystem {
    inner: Arc<FilesystemInner>,
}

struct FilesystemInner {
    onedrive: OneDrive,
    client: reqwest::Client,
    uid: u32,
    gid: u32,

    ino_counter: AtomicU64,
    ino_id_map: Mutex<InoIdMap>,

    fh_counter: AtomicU64,
    dir_content_map: Mutex<HashMap<u64, Vec<DirEntry>>>,
    file_download_url_map: Mutex<HashMap<u64, String>>,
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

struct DirEntry {
    ino: u64,
    kind: FileType,
    name: OsString,
}

impl Filesystem {
    pub fn new(onedrive: OneDrive, uid: u32, gid: u32) -> Self {
        Self {
            inner: Arc::new(FilesystemInner {
                onedrive,
                client: reqwest::Client::new(),
                uid,
                gid,
                ino_counter: 2.into(), // Skip FUSE_ROOT_ID.
                ino_id_map: Default::default(),
                fh_counter: 1.into(),
                dir_content_map: Default::default(),
                file_download_url_map: Default::default(),
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
                let ino = self.ino_counter.fetch_add(1, Ordering::Relaxed);
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

    fn item_attr_fields(&self) -> &'static [resource::DriveItemField] {
        use resource::DriveItemField;
        &[
            DriveItemField::folder,
            DriveItemField::size,
            DriveItemField::created_date_time,
            DriveItemField::last_modified_date_time,
        ]
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
            perm: 0o777,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
        }
    }

    fn dir_entry_fields(&self) -> &'static [resource::DriveItemField] {
        use resource::DriveItemField;
        &[
            DriveItemField::id,
            DriveItemField::folder,
            DriveItemField::name,
        ]
    }

    async fn alloc_dir_content(&self, items: Vec<resource::DriveItem>) -> u64 {
        let mut buf = Vec::with_capacity(items.len());
        for item in items {
            let ino = self.get_or_alloc_ino(item.id.unwrap()).await;
            let kind = match item.folder {
                Some(_) => FileType::Directory,
                None => FileType::RegularFile,
            };
            let name = item.name.unwrap().into();
            buf.push(DirEntry { ino, kind, name });
        }

        let fh = self.fh_counter.fetch_add(1, Ordering::Relaxed);
        self.dir_content_map.lock().await.insert(fh, buf);
        fh
    }

    async fn free_dir_content(&self, fh: u64) {
        self.dir_content_map.lock().await.remove(&fh);
    }

    async fn alloc_file_download_url(&self, url: String) -> u64 {
        let fh = self.fh_counter.fetch_add(1, Ordering::Relaxed);
        self.file_download_url_map.lock().await.insert(fh, url);
        fh
    }

    async fn free_file_download_url(&self, fh: u64) {
        self.file_download_url_map.lock().await.remove(&fh);
    }

    async fn read_file(&self, fh: u64, offset: u64, size: u32) -> anyhow::Result<bytes::Bytes> {
        use anyhow::{ensure, Context as _};

        let begin = offset;
        let end = offset
            .checked_add(u64::from(size))
            .and_then(|x| x.checked_add(1))
            .context("End offset overflow")?;

        let url = self.file_download_url_map.lock().await[&fh].clone();
        let resp = self
            .client
            .get(&url)
            .header(http::header::RANGE, format!("bytes={}-{}", begin, end))
            .send()
            .await?;
        ensure!(
            resp.status() == StatusCode::PARTIAL_CONTENT,
            "HTTP error: {}",
            resp.status(),
        );

        Ok(resp.bytes().await?)
    }
}

// FIXME: Owned version of `FileName`.
macro_rules! cvt_name {
    ($name:ident; $reply:expr) => {
        let $name = match cvt_name(&$name) {
            Some(name) => name,
            None => return $reply.error(libc::ENOENT),
        };
    };
}

// FIXME: Owned version of `ItemLocation`.
macro_rules! item_loc {
    ($loc:ident = #$parent:ident; $inner:expr, $reply:expr) => {
        let item_id;
        let $loc = if $parent == FUSE_ROOT_ID {
            ItemLocation::root()
        } else {
            item_id = $inner.get_item_id($parent).await.unwrap();
            ItemLocation::from_id(&item_id)
        };
    };
    ($loc:ident = #$parent:ident / $name:ident; $inner:expr, $reply:expr) => {
        cvt_name!($name; $reply);
        let path;
        let parent_item_id;
        let $loc = if $parent == FUSE_ROOT_ID {
            path = format!("/{}", $name.as_str());
            ItemLocation::from_path(&path).unwrap()
        } else {
            parent_item_id = $inner.get_item_id($parent).await.unwrap();
            ItemLocation::child_of_id(&parent_item_id, $name)
        };
    };
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

            item_loc!(loc = #parent / name; inner, reply);

            let item = match inner
                .onedrive
                .get_item_with_option(
                    loc,
                    ObjectOption::new()
                        .select(inner.item_attr_fields())
                        .select(&[DriveItemField::id]),
                )
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
            item_loc!(loc = #ino; inner, reply);

            let item = match inner
                .onedrive
                .get_item_with_option(loc, ObjectOption::new().select(inner.item_attr_fields()))
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

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        use onedrive_api::{option::DriveItemPutOption, ConflictBehavior};
        debug!("mkdir #{}/{}", parent, name.to_string_lossy());

        let name = name.to_owned();
        self.spawn(|inner| async move {
            cvt_name!(name; reply);
            item_loc!(parent_loc = #parent; inner, reply);

            let ret_item = match inner
                .onedrive
                .create_folder_with_option(
                    parent_loc,
                    name,
                    DriveItemPutOption::new().conflict_behavior(ConflictBehavior::Fail),
                )
                .await
            {
                Ok(item) => item,
                Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                    return reply.error(libc::ENOENT)
                }
                Err(err) if err.status_code() == Some(StatusCode::CONFLICT) => {
                    return reply.error(libc::EEXIST)
                }
                Err(err) => {
                    error!("mkdir: {}", err);
                    return reply.error(libc::EIO);
                }
            };

            let mut attr = inner.parse_item_attr(0, &ret_item);
            attr.ino = inner.get_or_alloc_ino(ret_item.id.unwrap()).await;
            let ttl = Timespec::new(0, 0);
            reply.entry(&ttl, &attr, GENERATION);
        });
    }

    fn opendir(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("opendir: #{} flags={}", ino, flags);
        self.spawn(|inner| async move {
            item_loc!(loc = #ino; inner, reply);

            let fetcher = match inner
                .onedrive
                .list_children_with_option(
                    loc,
                    CollectionOption::new().select(inner.dir_entry_fields()),
                )
                .await
            {
                Ok(fetcher) => fetcher.unwrap(),
                Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                    return reply.error(libc::ENOENT);
                }
                Err(err) => {
                    error!("opendir: {}", err);
                    return reply.error(libc::EIO);
                }
            };

            // FIXME: Slow. Large memory cost.
            let items = match fetcher.fetch_all(&inner.onedrive).await {
                Ok(items) => items,
                Err(err) => {
                    error!("opendir: {}", err);
                    return reply.error(libc::EIO);
                }
            };

            let fh = inner.alloc_dir_content(items).await;
            reply.opened(fh, 0);
        });
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir: #{} fh={} offset={}", ino, fh, offset);
        self.spawn(|inner| async move {
            let mut cnt = 0usize;
            for (idx, ent) in inner.dir_content_map.lock().await[&fh][offset as usize..]
                .iter()
                .enumerate()
            {
                if reply.add(ent.ino, 1 + idx as i64, ent.kind, &ent.name) {
                    break;
                }
                cnt += 1;
            }
            debug!("readdir: feed {} entries", cnt);
            reply.ok();
        });
    }

    fn releasedir(&mut self, _req: &Request, ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        debug!("releasedir: #{}", ino);
        self.spawn(|inner| async move {
            inner.free_dir_content(fh).await;
            reply.ok();
        });
    }

    fn fsyncdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("open: #{} flags={}", ino, flags);

        if (flags & libc::O_CREAT as u32) != 0
            || (flags & libc::O_WRONLY as u32) != 0
            || (flags & libc::O_RDWR as u32) != 0
        {
            return reply.error(libc::EPERM);
        }

        self.spawn(|inner| async move {
            item_loc!(loc = #ino; inner, reply);

            let download_url = match inner.onedrive.get_item_download_url(loc).await {
                Ok(url) => url,
                Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                    return reply.error(libc::ENOENT);
                }
                Err(err) => {
                    error!("open: {}", err);
                    return reply.error(libc::EIO);
                }
            };

            let fh = inner.alloc_file_download_url(download_url).await;
            reply.opened(fh, libc::O_RDONLY as u32);
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        debug!("read #{} fh={} offset={} size={}", ino, fh, offset, size);
        self.spawn(|inner| async move {
            match inner.read_file(fh, offset as u64, size).await {
                Ok(data) => reply.data(&*data),
                Err(err) => {
                    error!("read: {}", err);
                    reply.error(libc::EIO);
                }
            }
        });
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("release #{} fh={}", ino, fh);
        self.spawn(|inner| async move {
            inner.free_file_download_url(fh).await;
            reply.ok();
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink #{}/{}", parent, name.to_string_lossy());
        let name = name.to_owned();
        self.spawn(|inner| async move {
            item_loc!(loc = #parent / name; inner, reply);

            match inner.onedrive.delete(loc).await {
                Ok(()) => {}
                Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                    return reply.error(libc::ENOENT)
                }
                Err(err) => {
                    error!("unlink: {}", err);
                    return reply.error(libc::EIO);
                }
            }

            reply.ok();
        })
    }

    fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.unlink(req, parent, name, reply);
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn link(
        &mut self,
        _req: &Request,
        _ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EPERM);
    }

    fn symlink(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EPERM);
    }

    fn readlink(&mut self, _req: &Request, _ino: u64, reply: ReplyData) {
        reply.error(libc::EPERM);
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEmpty,
    ) {
        use onedrive_api::{option::DriveItemPutOption, ConflictBehavior};

        debug!(
            "rename #{}/{} -> #{}/{}",
            parent,
            name.to_string_lossy(),
            new_parent,
            new_name.to_string_lossy(),
        );

        let name = name.to_owned();
        let new_name = new_name.to_owned();
        self.spawn(|inner| async move {
            item_loc!(loc = #parent / name; inner, reply);
            item_loc!(new_parent = #new_parent; inner, reply);
            cvt_name!(new_name; reply);

            match inner
                .onedrive
                .move_with_option(
                    loc,
                    new_parent,
                    Some(new_name),
                    DriveItemPutOption::new().conflict_behavior(ConflictBehavior::Fail),
                )
                .await
            {
                Ok(_) => {}
                Err(err) if err.status_code() == Some(StatusCode::NOT_FOUND) => {
                    return reply.error(libc::ENOENT);
                }
                Err(err) => {
                    error!("rename: {}", err);
                    return reply.error(libc::EIO);
                }
            }
            reply.ok();
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
