use crate::{config::PermissionConfig, vfs};
use fuser::{
    FileAttr, FileType, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use std::{convert::TryFrom as _, ffi::OsStr, sync::Arc, time::SystemTime};

const GENERATION: u64 = 0;
const NAME_LEN: u32 = 2048;
const BLOCK_SIZE: u32 = 512;
const FRAGMENT_SIZE: u32 = 512;

const READDIR_CHUNK_SIZE: usize = 64;

pub struct Filesystem {
    inner: Arc<FilesystemInner>,
}

struct FilesystemInner {
    vfs: Arc<vfs::Vfs>,
    perm_config: PermissionConfig,
}

impl Filesystem {
    pub fn new(vfs: Arc<vfs::Vfs>, perm_config: PermissionConfig) -> Self {
        Self {
            inner: Arc::new(FilesystemInner { vfs, perm_config }),
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
    fn cvt_attr(&self, ino: u64, attr: vfs::InodeAttr) -> FileAttr {
        FileAttr {
            ino,
            size: attr.size,
            blocks: to_blocks_ceil(attr.size),
            atime: attr.mtime, // No info.
            mtime: attr.mtime,
            ctime: attr.mtime, // No info.
            crtime: attr.crtime,
            kind: if attr.is_directory {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: if attr.is_directory {
                self.perm_config.dir_permission()
            } else {
                self.perm_config.file_permission()
            } as _,
            nlink: 1,
            uid: self.perm_config.uid as _,
            gid: self.perm_config.gid as _,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }
}

impl fuser::Filesystem for Filesystem {
    fn init(
        &mut self,
        _req: &Request,
        _config: &mut KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        log::info!("FUSE initialized");
        let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]);
        Ok(())
    }

    fn destroy(&mut self) {
        log::info!("FUSE destroyed");
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        self.spawn(|inner| async move {
            match inner.vfs.statfs().await {
                Err(err) => reply.error(err.into_c_err()),
                Ok(vfs::StatfsData { total, free }) => reply.statfs(
                    to_blocks_ceil(total),
                    to_blocks_floor(free),
                    to_blocks_floor(free),
                    0,
                    0,
                    BLOCK_SIZE,
                    NAME_LEN,
                    FRAGMENT_SIZE,
                ),
            }
        });
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.lookup(parent, &name).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok((ino, attr, ttl)) => {
                    let attr = inner.cvt_attr(ino, attr);
                    reply.entry(&ttl, &attr, GENERATION);
                }
            }
        });
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        self.spawn(|inner| async move {
            inner.vfs.forget(ino, nlookup).await.unwrap();
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        self.spawn(|inner| async move {
            match inner.vfs.get_attr(ino).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok((attr, ttl)) => {
                    let attr = inner.cvt_attr(ino, attr);
                    reply.attr(&ttl, &attr);
                }
            }
        });
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        // FIXME: Check flags?
        self.spawn(|inner| async move {
            match inner.vfs.open_dir(ino).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok(fh) => reply.opened(fh, 0),
            }
        });
    }

    fn releasedir(&mut self, _req: &Request, ino: u64, fh: u64, _flags: i32, reply: ReplyEmpty) {
        self.spawn(|inner| async move {
            inner.vfs.close_dir(ino, fh).await.unwrap();
            reply.ok();
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
        let offset = u64::try_from(offset).unwrap();
        self.spawn(|inner| async move {
            match inner
                .vfs
                .read_dir(ino, fh, offset, READDIR_CHUNK_SIZE)
                .await
            {
                Err(err) => reply.error(err.into_c_err()),
                Ok(entries) => {
                    for (idx, entry) in entries.as_ref().iter().enumerate() {
                        let next_offset = offset
                            .checked_add(u64::try_from(idx).unwrap())
                            .unwrap()
                            .checked_add(1)
                            .unwrap();
                        let kind = if entry.attr.is_directory {
                            FileType::Directory
                        } else {
                            FileType::RegularFile
                        };
                        // Inode id here is useless and further `lookup` will still be called.
                        // But it still need to be not zero.
                        if reply.add(u64::MAX, next_offset as i64, kind, &entry.name) {
                            break;
                        }
                    }
                    reply.ok();
                }
            }
        });
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        // Read is always allowed.
        static_assertions::const_assert_eq!(libc::O_RDONLY, 0);
        log::trace!("open flags: {:#x}", flags);

        let write = (flags & libc::O_WRONLY) != 0;
        assert_eq!(flags & libc::O_TRUNC, 0);
        let ret_flags = flags & libc::O_WRONLY;

        self.spawn(|inner| async move {
            match inner.vfs.open_file(ino, write).await {
                Ok(fh) => reply.opened(fh, ret_flags as u32),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        log::trace!("open flags: {:#x}", flags);

        let _write = (flags & libc::O_WRONLY) != 0;
        let exclusive = (flags & libc::O_EXCL) != 0;
        let truncate = (flags & libc::O_TRUNC) != 0;
        let ret_flags = flags & (libc::O_WRONLY | libc::O_EXCL | libc::O_TRUNC);

        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner
                .vfs
                .open_create_file(parent, &name, truncate, exclusive)
                .await
            {
                Ok((ino, fh, attr, ttl)) => {
                    let attr = inner.cvt_attr(ino, attr);
                    reply.created(&ttl, &attr, GENERATION, fh, ret_flags as u32)
                }
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.spawn(|inner| async move {
            match inner.vfs.close_file(ino, fh).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let offset = u64::try_from(offset).unwrap();
        let size = usize::try_from(size).unwrap();
        self.spawn(|inner| async move {
            match inner.vfs.read_file(ino, fh, offset, size).await {
                Ok(data) => {
                    let data = data.as_ref();
                    reply.data(data);
                }
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.create_dir(parent, &name).await {
                Ok((ino, attr, ttl)) => {
                    let attr = inner.cvt_attr(ino, attr);
                    reply.entry(&ttl, &attr, GENERATION)
                }
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        // TODO: Handle flags.
        let name = name.to_owned();
        let newname = newname.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.rename(parent, &name, newparent, &newname).await {
                Ok(_) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.remove_dir(parent, &name).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.remove_file(parent, &name).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let data = data.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.write_file(ino, fh, offset as u64, &data).await {
                // > Write should return exactly the number of bytes requested except on error.
                Ok(()) => reply.written(data.len() as u32),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        self.spawn(|inner| async move {
            let mtime = mtime.map(|time| match time {
                TimeOrNow::SpecificTime(time) => time,
                TimeOrNow::Now => SystemTime::now(),
            });
            match inner.vfs.set_attr(ino, size, mtime).await {
                Ok((attr, ttl)) => {
                    let attr = inner.cvt_attr(ino, attr);
                    reply.attr(&ttl, &attr)
                }
                Err(err) => reply.error(err.into_c_err()),
            }
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
        // Currently we don't delay inode changes, so this is trivial.
        reply.ok();
    }

    fn fsync(&mut self, _req: &Request, ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        self.spawn(|inner| async move {
            match inner.vfs.sync_file(ino).await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into_c_err()),
            }
        });
    }
}

fn to_blocks_ceil(bytes: u64) -> u64 {
    (bytes + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64
}

fn to_blocks_floor(bytes: u64) -> u64 {
    bytes / BLOCK_SIZE as u64
}
