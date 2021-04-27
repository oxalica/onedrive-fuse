use crate::login::ManagedOnedrive;
use onedrive_api::{resource::DriveItem, FileName, OneDrive};
use reqwest::Client;
use serde::Deserialize;
use std::{
    ffi::OsStr,
    ops::Deref,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

pub mod error;
mod file;
mod inode;
mod inode_id;
mod statfs;
mod tracker;

pub use error::{Error, Result};
pub use inode::{DirEntry, InodeAttr};
pub use statfs::StatfsData;

#[derive(Debug, Deserialize)]
pub struct Config {
    statfs: statfs::Config,
    inode: inode::Config,
    file: file::Config,
    tracker: tracker::Config,
}

#[derive(Debug)]
pub enum UpdateEvent {
    /// Batch update from old states.
    BatchUpdate(Vec<DriveItem>),
    /// Update attribute of a single file due to modification.
    UpdateFile(file::UpdatedFileAttr),
}

pub struct Vfs {
    statfs: statfs::Statfs,
    id_pool: inode_id::InodeIdPool,
    inode_pool: inode::InodePool,
    file_pool: file::FilePool,
    tracker: tracker::Tracker,
    onedrive: ManagedOnedrive,
    client: Client,
    readonly: bool,
}

impl Vfs {
    pub async fn new(
        root_ino: u64,
        readonly: bool,
        config: Config,
        onedrive: ManagedOnedrive,
    ) -> anyhow::Result<Arc<Self>> {
        let (event_tx, event_rx) = mpsc::channel(1);
        let (init_tx, init_rx) = oneshot::channel();
        let tracker = tracker::Tracker::new(
            event_tx.clone(),
            inode::InodePool::SYNC_SELECT_FIELDS
                .iter()
                .chain(file::FilePool::SYNC_SELECT_FIELDS)
                .copied()
                .collect(),
            onedrive.clone(),
            config.tracker,
        )
        .await?;

        let this = Arc::new(Self {
            statfs: statfs::Statfs::new(config.statfs),
            id_pool: inode_id::InodeIdPool::new(root_ino),
            inode_pool: inode::InodePool::new(config.inode),
            file_pool: file::FilePool::new(event_tx, config.file)?,
            tracker,
            onedrive,
            client: Client::new(),
            readonly,
        });

        tokio::task::spawn(Self::sync_thread(Arc::downgrade(&this), event_rx, init_tx));
        // Wait for initialization.
        init_rx.await.expect("Initialization failed");
        Ok(this)
    }

    async fn sync_thread(
        this: Weak<Self>,
        mut event_rx: mpsc::Receiver<UpdateEvent>,
        init_tx: oneshot::Sender<()>,
    ) {
        let mut init_tx = Some(init_tx);
        while let Some(event) = event_rx.recv().await {
            let this = match this.upgrade() {
                Some(this) => this,
                None => return,
            };

            match event {
                UpdateEvent::BatchUpdate(updated) => {
                    this.inode_pool.sync_items(&updated);
                    this.file_pool.sync_items(&updated).await;

                    if let Some(init_tx) = init_tx.take() {
                        let root_id = updated
                            .iter()
                            .find(|item| item.root.is_some())
                            .expect("No root item found")
                            .id
                            .as_ref()
                            .expect("Missing id");
                        this.id_pool.set_root_item_id(root_id.clone());

                        if init_tx.send(()).is_err() {
                            return;
                        }
                    }
                }
                UpdateEvent::UpdateFile(updated) => {
                    this.inode_pool
                        .update_attr(&updated.item_id, |attr| InodeAttr {
                            size: updated.size,
                            mtime: updated.mtime,
                            c_tag: Some(updated.c_tag.clone().unwrap()),
                            dirty: updated.c_tag.is_none(),
                            ..attr
                        });
                }
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

    // Guard for write operation. Return error in readonly mode.
    fn write_guard(&self) -> Result<()> {
        if self.readonly {
            Err(Error::AccessDenied)
        } else {
            Ok(())
        }
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
        let parent_id = self.id_pool.get_item_id(parent_ino)?;
        let child_name = cvt_filename(child_name)?;
        let id = self.inode_pool.lookup(&parent_id, child_name)?;
        let attr = self.inode_pool.get_attr(&id)?;
        let ino = self.id_pool.acquire_or_alloc(&id);
        log::trace!(target: "vfs::inode", "lookup: id={:?} ino={} attr={:?}", id, ino, attr);
        Ok((ino, attr, self.ttl()))
    }

    pub async fn forget(&self, ino: u64, count: u64) -> Result<()> {
        let freed = self.id_pool.free(ino, count)?;
        log::trace!(target: "vfs::inode", "forget: ino={} count={} freed={}", ino, count, freed);
        Ok(())
    }

    pub async fn get_attr(&self, ino: u64) -> Result<(InodeAttr, Duration)> {
        let id = self.id_pool.get_item_id(ino)?;
        let attr = self.inode_pool.get_attr(&id)?;
        log::trace!(target: "vfs::inode", "get_attr: id={:?} ino={} attr={:?}", id, ino, attr);
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
        let parent_id = self.id_pool.get_item_id(ino)?;
        let ret = self.inode_pool.read_dir(&parent_id, offset, count)?;
        log::trace!(target: "vfs::dir", "read_dir: ino={} offset={}", ino, offset);
        Ok(ret)
    }

    pub async fn open_file(&self, ino: u64, write: bool) -> Result<u64> {
        if write {
            self.write_guard()?;
        }
        let item_id = self.id_pool.get_item_id(ino)?;
        let fh = self
            .file_pool
            .open(&item_id, write, &*self.onedrive().await, &self.client)
            .await?;
        log::trace!(target: "vfs::file", "open_file: ino={} fh={}", ino, fh);
        Ok(fh)
    }

    pub async fn open_create_file(
        &self,
        parent_ino: u64,
        child_name: &OsStr,
        truncate: bool,
        exclusive: bool,
    ) -> Result<(u64, u64, InodeAttr, Duration)> {
        self.write_guard()?;
        let parent_id = self.id_pool.get_item_id(parent_ino)?;
        let child_name = cvt_filename(child_name)?;
        if !truncate {
            // FIXME: Not atomic.
            match self.inode_pool.lookup(&parent_id, child_name) {
                Ok(id) => {
                    if exclusive {
                        return Err(Error::FileExists);
                    }
                    let attr = self.inode_pool.get_attr(&id)?;
                    let ino = self.id_pool.acquire_or_alloc(&id);
                    let fh = self.open_file(ino, true).await?;
                    return Ok((ino, fh, attr, self.ttl()));
                }
                Err(Error::NotFound) => {}
                Err(err) => return Err(err),
            }
        }
        let (fh, item_id, attr) = self
            .file_pool
            .open_create_empty(&parent_id, child_name, &*self.onedrive().await)
            .await?;
        self.inode_pool
            .insert_item(parent_id.clone(), child_name, item_id.clone(), attr.clone());
        let ino = self.id_pool.acquire_or_alloc(&item_id);
        Ok((ino, fh, attr, self.ttl()))
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

    pub async fn create_dir(
        &self,
        parent_ino: u64,
        name: &OsStr,
    ) -> Result<(u64, InodeAttr, Duration)> {
        self.write_guard()?;
        let name = cvt_filename(name)?;
        let parent_id = self.id_pool.get_item_id(parent_ino)?;
        let (id, attr) = self
            .inode_pool
            .create_dir(&parent_id, name, &*self.onedrive().await)
            .await?;
        let ino = self.id_pool.acquire_or_alloc(&id);
        log::trace!(
            target: "vfs::dir",
            "create_dir: parent_id={:?} parent_ino={} name={} id={:?} ino={}",
            parent_id, parent_ino, name.as_str(), id, ino,
        );
        Ok((ino, attr, self.ttl()))
    }

    pub async fn rename(
        &self,
        parent_ino: u64,
        name: &OsStr,
        new_parent_ino: u64,
        new_name: &OsStr,
    ) -> Result<()> {
        self.write_guard()?;
        let name = cvt_filename(name)?;
        let new_name = cvt_filename(new_name)?;
        let parent_id = self.id_pool.get_item_id(parent_ino)?;
        let new_parent_id = self.id_pool.get_item_id(new_parent_ino)?;
        self.inode_pool
            .rename(
                &parent_id,
                name,
                &new_parent_id,
                new_name,
                &*self.onedrive().await,
            )
            .await?;
        log::trace!(
            target: "vfs::dir",
            "rename: parent_id={:?} parent_ino={} name={} new_parent_id={:?} new_parent_ino={} new_name={}",
            parent_id, parent_ino, name.as_str(),
            new_parent_id, new_parent_ino, new_name.as_str(),
        );
        Ok(())
    }

    pub async fn remove_dir(&self, parent_ino: u64, name: &OsStr) -> Result<()> {
        self.write_guard()?;
        let name = cvt_filename(name)?;
        let parent_id = self.id_pool.get_item_id(parent_ino)?;
        self.inode_pool
            .remove(&parent_id, name, true, &*self.onedrive().await)
            .await?;
        log::trace!(
            target: "vfs::dir",
            "remove_dir: parent_id={:?} parent_ino={} name={}",
            parent_id, parent_ino, name.as_str(),
        );
        Ok(())
    }

    pub async fn remove_file(&self, parent_ino: u64, name: &OsStr) -> Result<()> {
        self.write_guard()?;
        let name = cvt_filename(name)?;
        let parent_id = self.id_pool.get_item_id(parent_ino)?;
        self.inode_pool
            .remove(&parent_id, name, false, &*self.onedrive().await)
            .await?;
        log::trace!(
            target: "vfs::dir",
            "remove_file: parent_id={:?} parent_ino={} name={}",
            parent_id, parent_ino, name.as_str(),
        );
        Ok(())
    }

    pub async fn write_file(&self, ino: u64, fh: u64, offset: u64, data: &[u8]) -> Result<()> {
        self.write_guard()?;
        let updated = self
            .file_pool
            .write(fh, offset, data, self.onedrive.clone())
            .await?;
        self.inode_pool
            .update_attr(&updated.item_id, |attr| InodeAttr {
                size: updated.size,
                mtime: updated.mtime,
                dirty: true,
                ..attr
            });
        log::trace!(
            target: "vfs::file",
            "write_file: ino={} fh={} offset={} len={} updated_attr={:?}",
            ino, fh, offset, data.len(), updated,
        );
        Ok(())
    }
}

fn cvt_filename(name: &OsStr) -> Result<&FileName> {
    name.to_str()
        .and_then(FileName::new)
        .ok_or_else(|| Error::InvalidFileName(name.to_owned()))
}
