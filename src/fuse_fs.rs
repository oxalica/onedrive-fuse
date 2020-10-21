use crate::{
    error::{IntoCError as _, Result},
    vfs,
};
use fuse::*;
use onedrive_api::OneDrive;
use std::{convert::TryFrom as _, ffi::OsStr, sync::Arc};
use time::Timespec;

const GENERATION: u64 = 0;
const NAME_LEN: u32 = 2048;
const BLOCK_SIZE: u32 = 512;
const FRAGMENT_SIZE: u32 = 512;

pub struct Filesystem {
    inner: Arc<FilesystemInner>,
}

struct FilesystemInner {
    onedrive: OneDrive,
    uid: u32,
    gid: u32,
    vfs: vfs::Vfs,
}

impl Filesystem {
    pub async fn new(onedrive: OneDrive, uid: u32, gid: u32, config: vfs::Config) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(FilesystemInner {
                uid,
                gid,
                vfs: vfs::Vfs::new(config, &onedrive).await?,
                onedrive,
            }),
        })
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
            perm: 0o777,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
        }
    }
}

impl fuse::Filesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> std::result::Result<(), libc::c_int> {
        log::info!("FUSE initialized");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        log::info!("FUSE destroyed");
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        log::debug!("statfs");
        self.spawn(|inner| async move {
            match inner.vfs.statfs(&inner.onedrive).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok((vfs::StatfsData { total, free }, _ttl)) => reply.statfs(
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
        log::debug!("lookup #{}/{}", parent, name.to_string_lossy());
        let name = name.to_owned();
        self.spawn(|inner| async move {
            match inner.vfs.lookup(parent, &name, &inner.onedrive).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok((ino, attr, ttl)) => {
                    let ttl = dur_to_timespec(ttl);
                    let attr = inner.cvt_attr(ino, attr);
                    reply.entry(&ttl, &attr, GENERATION);
                }
            }
        });
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        log::debug!("forget #{} nlookup={}", ino, nlookup);
        self.spawn(|inner| async move {
            inner.vfs.forget(ino, nlookup).await.unwrap();
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        log::debug!("getattr #{}", ino);
        self.spawn(|inner| async move {
            match inner.vfs.get_attr(ino, &inner.onedrive).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok((attr, ttl)) => {
                    let ttl = dur_to_timespec(ttl);
                    let attr = inner.cvt_attr(ino, attr);
                    reply.attr(&ttl, &attr);
                }
            }
        });
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        // FIXME: Check flags?
        self.spawn(|inner| async move {
            match inner.vfs.open_dir(ino, &inner.onedrive).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok(fh) => {
                    log::debug!("opendir #{} flags=0x{:X}(0o{1:o}) -> {}", ino, flags, fh);
                    reply.opened(fh, 0)
                }
            }
        });
    }

    fn releasedir(&mut self, _req: &Request, ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        log::debug!("releasedir #{} fh={}", ino, fh);
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
            match inner.vfs.read_dir(ino, fh, offset).await {
                Err(err) => reply.error(err.into_c_err()),
                Ok(entries) => {
                    let mut count = 0usize;
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
                        count += 1;
                        // Inode id here is useless and further `lookup` will still be called.
                        // But it still need to be not zero.
                        if reply.add(u64::MAX, next_offset as i64, kind, &entry.name) {
                            break;
                        }
                    }
                    log::debug!(
                        "readdir #{} fh={} offset={} -> {} entries",
                        ino,
                        fh,
                        offset,
                        count,
                    );
                    reply.ok();
                }
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

fn dur_to_timespec(dur: std::time::Duration) -> Timespec {
    Timespec::new(dur.as_secs() as i64, dur.subsec_nanos() as i32)
}
