use super::vfs;
use fuse::*;
use libc::c_int;
use onedrive_api::OneDrive;
use std::sync::Arc;

const GENERATION: u64 = 0;
const NAME_LEN: u32 = 2048;
const BLOCK_SIZE: u32 = 512;
const FRAGMENT_SIZE: u32 = 512;

pub struct Filesystem {
    inner: Arc<FilesystemInner>,
}

struct FilesystemInner {
    onedrive: OneDrive,
    client: reqwest::Client,
    uid: u32,
    gid: u32,
    vfs: vfs::Vfs,
}

impl Filesystem {
    pub fn new(onedrive: OneDrive, uid: u32, gid: u32) -> Self {
        Self {
            inner: Arc::new(FilesystemInner {
                onedrive,
                client: reqwest::Client::new(),
                uid,
                gid,
                vfs: Default::default(),
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

impl fuse::Filesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        log::info!("initialize");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        log::info!("destroy");
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        log::debug!("statfs");
        self.spawn(|inner| async move {
            match inner.vfs.statfs(&inner.onedrive).await {
                Ok(vfs::Statfs { total, free }) => reply.statfs(
                    to_blocks_ceil(total),
                    to_blocks_floor(free),
                    to_blocks_floor(free),
                    0,
                    0,
                    BLOCK_SIZE as u32,
                    NAME_LEN,
                    FRAGMENT_SIZE,
                ),
                Err(err) => reply.error(err),
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
