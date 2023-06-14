use std::ffi::OsStr;
use std::ops::ControlFlow;

use fuser::{consts, Request};

use super::{Backend, Vfs};

const GENERATION: u64 = 0;

pub struct FuseFs<B>(pub Vfs<B>);

impl<B: Backend> fuser::Filesystem for FuseFs<B> {
    fn init(
        &mut self,
        _req: &Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
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

    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent_ino: u64,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let Some(name) = name.to_str() else { return reply.error(libc::ENOENT) };
        match self.0.lookup(parent_ino, name) {
            Ok((attr, ttl)) => reply.entry(&ttl, &attr, GENERATION),
            Err(err) => reply.error(err.into()),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        match self.0.getattr(ino) {
            Ok((attr, ttl)) => reply.attr(&ttl, &attr),
            Err(err) => reply.error(err.into()),
        }
    }

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

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let flags = (flags as u32) & (!libc::S_IFMT);
        // TODO: Write support.
        if flags != 0 {
            return reply.error(libc::EPERM);
        }
        match self.0.open_file(ino) {
            // Keep cache between `open` as long as the generation of the inode unchanged.
            Ok(()) => reply.opened(0, consts::FOPEN_NONSEEKABLE | consts::FOPEN_KEEP_CACHE),
            Err(err) => reply.error(err.into()),
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
}
