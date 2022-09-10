use crate::api;
use crate::config::PermissionConfig;
use crate::error::{Error, Result};
use crate::inode::{check_file_name, Ino, Inode, InodePool};
use crate::login::ManagedOnedrive;
use anyhow::Context;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyDirectoryPlus,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyXattr, Request,
};
use serde::Deserialize;
use std::ffi::OsStr;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BLOCK_SIZE: u64 = 512;
const GENERATION: u64 = 0;

// TODO
const TTL: Duration = Duration::MAX;

#[derive(Debug, Deserialize)]
pub struct Config {}

pub struct Vfs(Arc<Mutex<VfsInner>>);

struct VfsInner {
    permission: PermissionConfig,
    inodes: InodePool,
}

impl Vfs {
    // TODO
    pub async fn new(
        _config: Config,
        permission: PermissionConfig,
        onedrive: ManagedOnedrive,
        _client: reqwest::Client,
    ) -> anyhow::Result<Self> {
        let initial_items = {
            let onedrive = onedrive.get().await;
            let token = onedrive.access_token();
            let client = onedrive.client();

            log::info!("Fetching tree");
            let mut resp: api::DeltaResponse =
                api::DeltaRequest::initial().send(client, token).await?;
            let mut initial_items = Vec::new();
            let _delta_link = loop {
                log::info!("Got {} items", resp.value.len());
                initial_items.extend(
                    resp.value
                        .into_iter()
                        .filter(|item| item.special_folder.is_none()),
                );

                match (resp.next_link, resp.delta_link) {
                    (Some(next_link), _) => {
                        resp = api::DeltaRequest::link(next_link)
                            .send(client, token)
                            .await?;
                    }
                    (None, Some(delta_link)) => break delta_link,
                    (None, None) => return Err(Error::from("Missing delta link").into()),
                }
            };

            initial_items
        };

        let mut initial_items = initial_items.into_iter();
        let root_item = initial_items
            .next()
            .ok_or_else(|| Error::Protocol("Missing root".into()))?;
        let mut inodes = InodePool::new(root_item);
        for item in initial_items {
            inodes
                .apply_change(&item)
                .with_context(|| format!("When processing item: {:?}", item))?;
        }

        let inner = Arc::new(Mutex::new(VfsInner { inodes, permission }));
        Ok(Vfs(inner))
    }

    fn lock(&mut self) -> impl DerefMut<Target = VfsInner> + '_ {
        self.0.lock().unwrap()
    }
}

impl Filesystem for Vfs {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        log::info!("FUSE initialized");
        let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]);
        let _ = config.add_capabilities(fuser::consts::FUSE_DO_READDIRPLUS);
        let _ = config.add_capabilities(fuser::consts::FUSE_NO_OPENDIR_SUPPORT);
        Ok(())
    }

    fn destroy(&mut self) {
        log::info!("FUSE destroyed")
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match (|| -> Result<_> {
            let vfs = self.lock();
            let name = check_file_name(name)?;
            let ino = vfs.inodes.get(Ino(parent))?.data.get_child(name)?;
            let inode = vfs.inodes.get(ino)?;
            let attr = to_file_attr(ino, inode, &vfs.permission);
            Ok(attr)
        })() {
            Ok(attr) => reply.entry(&TTL, &attr, GENERATION),
            Err(e) => reply.error(e.into()),
        }
    }

    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {}

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match (|| -> Result<_> {
            let vfs = self.lock();
            let ino = Ino(ino);
            let inode = vfs.inodes.get(ino)?;
            let attr = to_file_attr(ino, inode, &vfs.permission);
            Ok(attr)
        })() {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(err) => reply.error(err.into()),
        };
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(libc::ENOTSUP);
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOTSUP);
    }

    fn listxattr(&mut self, _req: &Request<'_>, _ino: u64, _size: u32, reply: ReplyXattr) {
        reply.error(libc::ENOTSUP);
    }

    fn removexattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::ENOTSUP);
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok()
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let vfs = self.lock();
        match (|| vfs.inodes.get(Ino(ino))?.data.as_dir())() {
            Ok(children) => {
                for i in (offset as usize).. {
                    let (name, &ino) = match children.get_index(i) {
                        Some(ent) => ent,
                        None => break,
                    };
                    let child = vfs.inodes.get(ino).expect("Tree invariant");
                    let kind = match child.data.is_dir() {
                        true => FileType::Directory,
                        false => FileType::RegularFile,
                    };
                    if reply.add(ino.0, (i + 1) as i64, kind, name) {
                        break;
                    }
                }
                reply.ok()
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let vfs = self.lock();
        match (|| vfs.inodes.get(Ino(ino))?.data.as_dir())() {
            Ok(children) => {
                for i in (offset as usize).. {
                    let (name, &ino) = match children.get_index(i) {
                        Some(ent) => ent,
                        None => break,
                    };
                    let child = vfs.inodes.get(ino).expect("Tree invariant");
                    let attr = to_file_attr(ino, child, &vfs.permission);
                    if reply.add(ino.0, (i + 1) as i64, name, &TTL, &attr, GENERATION) {
                        break;
                    }
                }
                reply.ok()
            }
            Err(e) => reply.error(e.into()),
        }
    }
}

fn to_file_attr(ino: Ino, inode: &Inode, perm: &PermissionConfig) -> FileAttr {
    FileAttr {
        ino: ino.0,
        size: inode.size,
        blocks: (inode.size + (BLOCK_SIZE - 1)) / BLOCK_SIZE,
        // No data.
        atime: inode.last_modified_time,
        mtime: inode.last_modified_time,
        // No data.
        ctime: inode.last_modified_time,
        crtime: inode.created_time,
        kind: match inode.data.is_dir() {
            true => FileType::Directory,
            false => FileType::RegularFile,
        },
        perm: match inode.data.is_dir() {
            true => perm.dir_permission() as u16,
            false => perm.file_permission() as u16,
        },
        nlink: 1,
        uid: perm.uid,
        gid: perm.gid,
        rdev: 0,
        blksize: BLOCK_SIZE as u32,
        flags: 0,
    }
}
