use crate::login::ManagedOnedrive;
use onedrive_api::OneDrive;
use reqwest::Client;
use serde::Deserialize;
use std::{
    ffi::OsStr,
    ops::Deref,
    sync::{Arc, Mutex as SyncMutex, Weak},
    time::Duration,
};

mod dir;
pub mod error;
mod file;
mod inode;
mod statfs;
mod tracker;

pub use dir::DirEntry;
pub use error::{Error, Result};
pub use inode::InodeAttr;
pub use statfs::StatfsData;

#[derive(Debug, Deserialize)]
pub struct Config {
    statfs: statfs::Config,
    inode: inode::Config,
    dir: dir::Config,
    file: file::Config,
    tracker: tracker::Config,
}

pub struct Vfs {
    statfs: statfs::Statfs,
    inode_pool: inode::InodePool,
    dir_pool: dir::DirPool,
    file_pool: file::FilePool,
    tracker: tracker::Tracker,
    onedrive: ManagedOnedrive,
    client: Client,
}

impl Vfs {
    pub async fn new(config: Config, onedrive: ManagedOnedrive) -> anyhow::Result<Arc<Self>> {
        // Initialize tracker before everything else.
        let this_referrer = Arc::new(SyncMutex::new(None));
        let this_referrer2 = this_referrer.clone();
        let tracker = tracker::Tracker::new(
            Box::new(move |event| Box::pin(Self::sync_changes(this_referrer2.clone(), event))),
            onedrive.clone(),
            config.tracker,
        )
        .await?;

        let inode_pool = inode::InodePool::new(config.inode, &*onedrive.get().await).await?;
        let this = Arc::new(Self {
            statfs: statfs::Statfs::new(config.statfs),
            inode_pool,
            dir_pool: dir::DirPool::new(config.dir),
            file_pool: file::FilePool::new(config.file)?,
            tracker,
            onedrive,
            client: Client::new(),
        });
        *this_referrer.lock().unwrap() = Some(Arc::downgrade(&this));
        Ok(this)
    }

    async fn sync_changes(this: Arc<SyncMutex<Option<Weak<Self>>>>, event: tracker::Event) {
        let this = match &*this.lock().unwrap() {
            // FIXME
            None => panic!("Remote changed during initialization"),
            Some(weak) => match weak.upgrade() {
                Some(arc) => arc,
                None => return,
            },
        };

        // `FilePool` use download URL as snapshot and will not affected by changes.
        match event {
            tracker::Event::Clear => {
                log::debug!("Clear all cache");
                this.dir_pool.clear_cache();
                this.inode_pool.clear_cache();
            }
            tracker::Event::Update(items) => {
                this.dir_pool.sync_items(&items);
                this.inode_pool.sync_items(&items);
            }
        }
    }

    async fn onedrive(&self) -> impl Deref<Target = OneDrive> + '_ {
        self.onedrive.get().await
    }

    fn ttl(&self) -> Duration {
        // Use `i64::MAX` to avoid overflowing `libc::time_t`;
        const MAX_TTL: Duration = Duration::from_secs(i64::MAX as u64);
        self.tracker.time_to_next_sync().unwrap_or(MAX_TTL)
    }

    pub async fn statfs(&self) -> Result<(StatfsData, Duration)> {
        let (ret, ttl) = self.statfs.statfs(&*self.onedrive().await).await?;
        log::trace!(target: "vfs::statfs", "statfs: statfs={:?} ttl={:?}", ret, ttl);
        Ok((ret, ttl))
    }

    pub async fn lookup(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
    ) -> Result<(u64, InodeAttr, Duration)> {
        let (ino, attr) = self
            .inode_pool
            .lookup(
                parent_ino,
                child_name,
                &self.dir_pool,
                &*self.onedrive().await,
            )
            .await?;
        log::trace!(target: "vfs::inode", "lookup: ino={} attr={:?}", ino, attr);
        Ok((ino, attr, self.ttl()))
    }

    pub async fn forget(&self, ino: u64, count: u64) -> Result<()> {
        let freed = self.inode_pool.free(ino, count).await?;
        log::trace!(target: "vfs::inode", "forget: ino={} count={} freed={}", ino, count, freed);
        Ok(())
    }

    pub async fn get_attr(&self, ino: u64) -> Result<(InodeAttr, Duration)> {
        let attr = self
            .inode_pool
            .get_attr(ino, &self.dir_pool, &*self.onedrive().await)
            .await?;
        log::trace!(target: "vfs::inode", "get_attr: ino={} attr={:?}", ino, attr);
        Ok((attr, self.ttl()))
    }

    // fh is not used for directories.
    pub async fn open_dir(&self, ino: u64) -> Result<u64> {
        log::trace!(target: "vfs::dir", "open_dir: ino={}", ino);
        Ok(0)
    }

    // fh is not used for directories.
    pub async fn close_dir(&self, ino: u64, _fh: u64) -> Result<()> {
        log::trace!(target: "vfs::dir", "close_dir: ino={}", ino);
        Ok(())
    }

    pub async fn read_dir(
        &self,
        ino: u64,
        _fh: u64,
        offset: u64,
        count: usize,
    ) -> Result<impl AsRef<[DirEntry]>> {
        let parent_id = self.inode_pool.get_item_id(ino)?;
        let ret = self
            .dir_pool
            .read(&parent_id, offset, count, &*self.onedrive().await)
            .await?;
        log::trace!(target: "vfs::dir", "read_dir: ino={} offset={}", ino, offset);
        Ok(ret)
    }

    // TODO: Flags.
    pub async fn open_file(&self, ino: u64) -> Result<u64> {
        let item_id = self.inode_pool.get_item_id(ino)?;
        let fh = self
            .file_pool
            .open(&item_id, &*self.onedrive().await, &self.client)
            .await?;
        log::trace!(target: "vfs::file", "open_file: ino={} fh={}", ino, fh);
        Ok(fh)
    }

    pub async fn close_file(&self, ino: u64, fh: u64) -> Result<()> {
        self.file_pool.close(fh).await?;
        log::trace!(target: "vfs::file", "close_file: ino={} fh={}", ino, fh);
        Ok(())
    }

    pub async fn read_file(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        size: usize,
    ) -> Result<impl AsRef<[u8]>> {
        let ret = self.file_pool.read(fh, offset, size).await?;
        log::trace!(
            target: "vfs::file",
            "read_file: ino={} fh={} offset={} size={} bytes_read={}",
            ino,
            fh,
            offset,
            size,
            ret.as_ref().len(),
        );
        Ok(ret)
    }
}
