use std::ffi::OsStr;
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use fuser::{consts, Request};
use nix::fcntl::OFlag;

use super::{Backend, Vfs};

const GENERATION: u64 = 0;

pub struct FuseFs<B>(pub Vfs<B>);

impl<B: Backend> fuser::Filesystem for FuseFs<B> {
    // Entry and exit.

    fn init(
        &mut self,
        _req: &Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        config
            .set_time_granularity(Duration::from_secs(1))
            .expect("failed to set time granularity");

        // Prefer readdirplus if possible.
        let _ = config.add_capabilities(consts::FUSE_DO_READDIRPLUS);
        // Stateless readdir.
        let _ = config.add_capabilities(consts::FUSE_PARALLEL_DIROPS);

        // Multi-issue `read` is not supported.
        let _ = config.set_max_background(1);

        log::info!("Filesystem initialized");
        Ok(())
    }

    fn destroy(&mut self) {
        log::info!("Filesystem destroyed");
    }

    // Lookup and attributes.

    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent_ino: u64,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let Some(name) = name.to_str() else {
            return reply.error(libc::ENOENT);
        };
        match self.0.lookup(parent_ino, name) {
            Ok((attr, ttl)) => reply.entry(&ttl, &attr, GENERATION),
            Err(err) => reply.error(err.into()),
        }
    }

    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {}

    fn batch_forget(&mut self, _req: &Request<'_>, _nodes: &[fuser::fuse_forget_one]) {}

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        match self.0.get_attr(ino) {
            Ok((attr, ttl)) => reply.attr(&ttl, &attr),
            Err(err) => reply.error(err.into()),
        }
    }

    fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        if mode.is_some() || uid.is_some() || gid.is_some() || flags.is_some() {
            return reply.error(libc::EOPNOTSUPP);
        }
        if size.is_some() {
            // TODO: Truncation.
            return reply.error(libc::EOPNOTSUPP);
        }
        if crtime.is_some() || mtime.is_some() {
            let mtime = mtime.map(|t| match t {
                fuser::TimeOrNow::SpecificTime(t) => t,
                fuser::TimeOrNow::Now => SystemTime::now(),
            });
            return match self.0.set_time(ino, crtime, mtime) {
                Ok((ttl, attr)) => reply.attr(&ttl, &attr),
                Err(err) => reply.error(err.into()),
            };
        }
        self.getattr(req, ino, reply);
    }

    // Directory operations.

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        match self.0.read_dir(ino, offset as u64, |name, attr, _ttl| {
            if reply.add(attr.ino, attr.ino as i64, attr.kind, name) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err.into()),
        }
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectoryPlus,
    ) {
        match self.0.read_dir(ino, offset as u64, |name, attr, ttl| {
            if reply.add(attr.ino, attr.ino as i64, name, &ttl, &attr, GENERATION) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err.into()),
        }
    }

    fn fsyncdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        self.fsync(req, ino, fh, datasync, reply);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent_ino: u64,
        child_name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let Some(child_name) = child_name.to_str() else {
            return reply.error(libc::EINVAL);
        };
        match self.0.create_directory(parent_ino, child_name) {
            Ok((ttl, attr)) => reply.entry(&ttl, &attr, GENERATION),
            Err(err) => reply.error(err.into()),
        }
    }

    // File operations.

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        const SUPPORTED_FLAGS: OFlag = OFlag::O_LARGEFILE.union(OFlag::O_ACCMODE);

        let Some(flags) = OFlag::from_bits(flags).filter(|&f| SUPPORTED_FLAGS.contains(f)) else {
            return reply.error(libc::EOPNOTSUPP);
        };
        match flags & OFlag::O_ACCMODE {
            OFlag::O_RDONLY => {
                match self.0.open_file(ino) {
                    // Keep cache between `open` as long as the generation of the inode unchanged.
                    Ok(()) => reply.opened(0, consts::FOPEN_NONSEEKABLE | consts::FOPEN_KEEP_CACHE),
                    Err(err) => reply.error(err.into()),
                }
            }
            // TODO: Write support.
            _ => reply.error(libc::EOPNOTSUPP),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        self.0.close_file(ino);
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        match self
            .0
            .read_file(ino, offset as u64, size.try_into().unwrap())
        {
            Ok(fut) => {
                tokio::spawn(async move {
                    match fut.await {
                        Ok(buf) => reply.data(&buf),
                        Err(err) => reply.error(err.into()),
                    }
                });
            }
            Err(err) => reply.error(err.into()),
        }
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let fut = self.0.sync();
        tokio::spawn(async move {
            match fut.await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into()),
            }
        });
    }
}
