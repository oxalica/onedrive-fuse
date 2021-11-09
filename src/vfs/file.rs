use crate::{
    config::de_duration_sec,
    login::ManagedOnedrive,
    paths::default_disk_cache_dir,
    vfs::{Error, Result, UpdateEvent},
};
use bytes::{Bytes, BytesMut};
use lru_cache::LruCache;
use onedrive_api::{
    option::DriveItemPutOption,
    resource::{DriveItem, DriveItemField},
    ConflictBehavior, ItemId, ItemLocation, OneDrive, Tag,
};
use reqwest::{header, StatusCode};
use serde::Deserialize;
use sharded_slab::Slab;
use std::{
    convert::TryFrom as _,
    io::{self, SeekFrom},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as SyncMutex, Weak,
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc, oneshot, watch, Mutex, MutexGuard},
    time,
};

use super::InodeAttr;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    disk_cache: DiskCacheConfig,
    download: DownloadConfig,
    upload: UploadConfig,
}

#[derive(Debug, Deserialize, Clone)]
struct DownloadConfig {
    max_retry: usize,
    #[serde(deserialize_with = "de_duration_sec")]
    retry_delay: Duration,
    stream_buffer_chunks: usize,
    stream_ring_buffer_size: usize,
    #[serde(deserialize_with = "de_duration_sec")]
    chunk_timeout: Duration,
}

#[derive(Debug, Deserialize, Clone)]
struct DiskCacheConfig {
    enable: bool,
    #[serde(default = "default_disk_cache_dir")]
    path: PathBuf,
    max_cached_file_size: u64,
    max_files: usize,
    max_total_size: u64,
}

#[derive(Debug, Deserialize, Clone)]
struct UploadConfig {
    max_size: u64,
    #[serde(deserialize_with = "de_duration_sec")]
    flush_delay: Duration,
    #[serde(deserialize_with = "de_duration_sec")]
    retry_delay: Duration,
}

pub struct FilePool {
    handles: Slab<File>,
    disk_cache: Option<DiskCache>,
    event_tx: mpsc::Sender<UpdateEvent>,
    config: Config,
    onedrive: ManagedOnedrive,
    /// The client without timeout limit, which is used for upload and download.
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct UpdatedFileAttr {
    pub item_id: ItemId,
    pub size: u64,
    pub mtime: SystemTime,
    pub c_tag: Tag,
}

#[derive(Debug, Clone)]
struct RemoteFileMeta {
    size: u64,
    c_tag: Tag,
    download_url: String,
}

impl FilePool {
    pub const SYNC_SELECT_FIELDS: &'static [DriveItemField] = &[DriveItemField::c_tag];

    pub fn new(
        event_tx: mpsc::Sender<UpdateEvent>,
        onedrive: ManagedOnedrive,
        unlimit_client: reqwest::Client,
        config: Config,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            handles: Slab::new(),
            disk_cache: if config.disk_cache.enable {
                Some(DiskCache::new(config.clone())?)
            } else {
                None
            },
            event_tx,
            config,
            onedrive,
            client: unlimit_client,
        })
    }

    fn key_to_fh(key: usize) -> u64 {
        u64::try_from(key).unwrap()
    }

    fn fh_to_key(fh: u64) -> usize {
        usize::try_from(fh).unwrap()
    }

    // Fetch file size, CTag and download URL.
    async fn fetch_meta(item_id: &ItemId, onedrive: &OneDrive) -> Result<RemoteFileMeta> {
        // `download_url` is available without `$select`.
        let item = onedrive.get_item(ItemLocation::from_id(item_id)).await?;
        Ok(RemoteFileMeta {
            size: item.size.unwrap() as u64,
            c_tag: item.c_tag.unwrap(),
            download_url: item.download_url.unwrap(),
        })
    }

    async fn open_inner(&self, item_id: &ItemId, write_mode: bool) -> Result<File> {
        let meta = if let Some(cache) = &self.disk_cache {
            if let Some(state) = cache.get(item_id) {
                log::debug!("File already cached: {:?}", item_id);
                return Ok(File::Cached(state));
            }

            let meta = Self::fetch_meta(item_id, &*self.onedrive.get().await).await?;
            if let Some(state) = cache.try_alloc_and_fetch(
                item_id,
                &meta,
                None,
                self.onedrive.clone(),
                self.event_tx.clone(),
                self.client.clone(),
            )? {
                log::debug!("Caching file {:?}, meta: {:?}", item_id, meta);
                return Ok(File::Cached(state));
            } else if write_mode {
                return Err(Error::FileTooLarge);
            }

            meta
        } else if write_mode {
            return Err(Error::WriteWithoutCache);
        } else {
            Self::fetch_meta(item_id, &*self.onedrive.get().await).await?
        };

        log::debug!("Streaming file {:?}, meta: {:?}", item_id, meta);
        let state =
            FileStreamState::fetch(&meta, self.client.clone(), self.config.download.clone());
        Ok(File::Streaming(Arc::new(Mutex::new(state))))
    }

    pub async fn open(&self, item_id: &ItemId, write_mode: bool) -> Result<u64> {
        let file = self.open_inner(item_id, write_mode).await?;
        let key = self.handles.insert(file).expect("Pool is full");
        Ok(Self::key_to_fh(key))
    }

    pub async fn open_create_empty(
        &self,
        item_loc: ItemLocation<'_>,
    ) -> Result<(u64, ItemId, InodeAttr)> {
        let cache = self.disk_cache.as_ref().ok_or(Error::WriteWithoutCache)?;

        let item = self
            .onedrive
            .get()
            .await
            .upload_small(item_loc, Vec::new())
            .await?;
        assert_eq!(item.size, Some(0));
        let attr = InodeAttr::parse_item(&item).expect("Invalid attrs");
        let id = item.id.expect("Missing id");
        log::debug!("Truncated or created file {:?}", id);

        let file = cache
            .insert_empty(id.clone(), attr.c_tag.clone().unwrap())
            .await?;
        let key = self
            .handles
            .insert(File::Cached(file))
            .expect("Pool is full");
        Ok((Self::key_to_fh(key), id, attr))
    }

    pub async fn truncate_file(
        &self,
        item_id: &ItemId,
        new_size: u64,
        mtime: SystemTime,
    ) -> Result<()> {
        if new_size > self.config.disk_cache.max_cached_file_size {
            return Err(Error::FileTooLarge);
        }

        let cache = self.disk_cache.as_ref().ok_or(Error::WriteWithoutCache)?;

        let file = cache.cache.lock().unwrap().get_mut(item_id).cloned();
        if let Some(file) = file {
            let mut guard = file.state.lock().await;
            match guard.status {
                FileCacheStatus::Downloading { truncate } => {
                    let download_size = truncate.map(|(sz, _)| sz).unwrap_or(guard.file_size);
                    guard.status = FileCacheStatus::Downloading {
                        truncate: Some((download_size.min(new_size), mtime)),
                    };
                    guard.file_size = new_size;
                    guard.cache_file.set_len(new_size).await.unwrap();
                    log::debug!(
                        "Pending another truncate for still downloading file {:?}",
                        item_id,
                    );
                    return Ok(());
                }
                FileCacheStatus::Available | FileCacheStatus::Dirty { .. } => {
                    log::debug!(
                        "Truncated cached file {:?}: {} -> {}",
                        item_id,
                        guard.file_size,
                        new_size,
                    );
                    guard.file_size = new_size;
                    guard.cache_file.set_len(new_size).await.unwrap();
                    file.queue_upload(
                        &mut guard,
                        mtime,
                        self.onedrive.clone(),
                        self.client.clone(),
                        self.event_tx.clone(),
                        self.config.upload.clone(),
                    );
                    return Ok(());
                }
                FileCacheStatus::DownloadFailed | FileCacheStatus::Invalidated => {}
            }
        }

        let meta = Self::fetch_meta(item_id, &*self.onedrive.get().await).await?;
        log::debug!(
            "Download with truncate {:?}: new size: {}, remote meta: {:?}",
            item_id,
            new_size,
            meta,
        );

        match cache.try_alloc_and_fetch(
            item_id,
            &meta,
            Some((new_size, mtime)),
            self.onedrive.clone(),
            self.event_tx.clone(),
            self.client.clone(),
        )? {
            Some(_) => Ok(()),
            None => Err(Error::FileTooLarge),
        }
    }

    pub async fn close(&self, fh: u64) -> Result<()> {
        if self.handles.remove(Self::fh_to_key(fh)) {
            Ok(())
        } else {
            Err(Error::InvalidHandle(fh))
        }
    }

    pub async fn read(&self, fh: u64, offset: u64, size: usize) -> Result<impl AsRef<[u8]>> {
        let file = self
            .handles
            .get(Self::fh_to_key(fh))
            .ok_or(Error::InvalidHandle(fh))?
            .clone();
        match file {
            File::Streaming(state) => state.lock().await.read(offset, size).await,
            File::Cached(state) => FileCache::read(&state, offset, size).await,
        }
    }

    /// Write to cached file. Returns item id and file size after the write.
    pub async fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<UpdatedFileAttr> {
        let file = self
            .handles
            .get(Self::fh_to_key(fh))
            .ok_or(Error::InvalidHandle(fh))?
            .clone();
        match file {
            File::Streaming { .. } => panic!("Cannot stream in write mode"),
            File::Cached(state) => {
                FileCache::write(
                    &state,
                    offset,
                    data,
                    self.event_tx.clone(),
                    self.onedrive.clone(),
                    self.client.clone(),
                    self.config.upload.clone(),
                )
                .await
            }
        }
    }

    pub async fn flush_file(&self, item_id: &ItemId) -> Result<()> {
        if let Some(cache) = &self.disk_cache {
            if let Some(file) = cache.get(item_id) {
                let mut guard = file.state.lock().await;
                match guard.status {
                    FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
                    FileCacheStatus::Available | FileCacheStatus::Invalidated => return Ok(()),
                    FileCacheStatus::Downloading { .. } => {
                        let mut rx = guard.available_size.clone();
                        drop(guard);
                        while rx.changed().await.is_ok() {}
                        guard = file.state.lock().await;
                    }
                    FileCacheStatus::Dirty { .. } => {}
                }
                loop {
                    let (flush_tx, mut done_rx) = match &mut guard.status {
                        FileCacheStatus::Downloading { .. } => unreachable!(),
                        FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
                        FileCacheStatus::Invalidated | FileCacheStatus::Available => return Ok(()),
                        FileCacheStatus::Dirty {
                            flush_tx, done_rx, ..
                        } => (flush_tx.take(), done_rx.clone()),
                    };
                    drop(guard);
                    if let Some(flush_tx) = flush_tx {
                        let _ = flush_tx.send(());
                    }
                    while done_rx.changed().await.is_ok() {}
                    // May be canceled by another modification during the upload.
                    if *done_rx.borrow() {
                        return Ok(());
                    }
                    guard = file.state.lock().await;
                }
            }
        }
        Ok(())
    }

    pub async fn sync_items(&self, items: &[DriveItem]) {
        if let Some(cache) = &self.disk_cache {
            cache.sync_items(items).await;
        }
    }
}

#[derive(Debug, Clone)]
enum File {
    Streaming(Arc<Mutex<FileStreamState>>),
    Cached(Arc<FileCache>),
}

#[derive(Debug)]
struct FileStreamState {
    file_size: u64,
    buf_start_pos: u64,
    buf: RingBuf,
    rx: mpsc::Receiver<Bytes>,
}

#[derive(Debug)]
struct RingBuf {
    v: Vec<u8>,
    l: usize,
    r: usize,
}

impl RingBuf {
    fn new(capacity: usize) -> Self {
        let v = vec![0u8; capacity.checked_add(1).unwrap()];
        Self { v, l: 0, r: 0 }
    }

    fn capacity(&self) -> usize {
        self.v.len() - 1
    }

    fn len(&self) -> usize {
        if self.l <= self.r {
            self.r - self.l
        } else {
            self.r + self.v.len() - self.l
        }
    }

    fn slice(&self, range: std::ops::Range<usize>) -> (&[u8], &[u8]) {
        assert!(range.start <= range.end && range.end <= self.len());
        let (start, end, l, wrap) = (range.start, range.end, self.l, self.v.len());
        if l + end <= wrap {
            (&self.v[(l + start)..(l + end)], &[])
        } else if wrap < l + start {
            (&self.v[(l + start - wrap)..(l + end - wrap)], &[])
        } else {
            (&self.v[(l + start)..], &self.v[..(l + end - wrap)])
        }
    }

    /// Return truncated bytes from left.
    fn feed(&mut self, data: &[u8]) -> usize {
        assert!(data.len() <= self.capacity());
        let truncate = (self.len() + data.len()).saturating_sub(self.capacity());
        let wrap = self.v.len();
        if self.l + truncate < wrap {
            self.l += truncate;
        } else {
            self.l = self.l + truncate - wrap;
        }
        if self.r + data.len() < wrap {
            self.v[self.r..(self.r + data.len())].copy_from_slice(data);
            self.r += data.len();
        } else {
            let rest = wrap - self.r;
            self.v[self.r..].copy_from_slice(&data[..rest]);
            self.v[..(data.len() - rest)].copy_from_slice(&data[rest..]);
            self.r = data.len() - rest;
        }
        truncate
    }
}

impl FileStreamState {
    fn fetch(meta: &RemoteFileMeta, client: reqwest::Client, config: DownloadConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.stream_buffer_chunks);
        let buf = RingBuf::new(config.stream_ring_buffer_size);
        tokio::spawn(download_thread(
            meta.size,
            meta.download_url.clone(),
            tx,
            client,
            config,
        ));
        Self {
            file_size: meta.size,
            buf_start_pos: 0,
            buf,
            rx,
        }
    }

    async fn read(&mut self, offset: u64, size: usize) -> Result<Bytes> {
        let size = (self.file_size.saturating_sub(offset)).min(size as u64) as usize;
        if size == 0 {
            return Ok(Bytes::new());
        }
        let end = offset + size as u64;

        while self.buf_start_pos + (self.buf.len() as u64) < end {
            let chunk = match self.rx.recv().await {
                Some(chunk) => chunk,
                None => return Err(Error::DownloadFailed),
            };
            let advance = self.buf.feed(&*chunk);
            self.buf_start_pos += advance as u64;
        }

        if offset < self.buf_start_pos {
            return Err(Error::NonsequentialRead {
                current_pos: self.buf_start_pos,
                read_offset: offset,
                read_size: size,
            });
        }

        let start = (offset - self.buf_start_pos) as usize;
        let (lhs, rhs) = self.buf.slice(start..(start + size));
        let mut ret = BytesMut::with_capacity(size);
        ret.extend_from_slice(lhs);
        ret.extend_from_slice(rhs);
        Ok(ret.freeze())
    }
}

async fn download_thread(
    file_size: u64,
    download_url: String,
    tx: mpsc::Sender<Bytes>,
    client: reqwest::Client,
    config: DownloadConfig,
) {
    let mut pos = 0u64;

    log::debug!("Start downloading ({} bytes)", file_size);

    while pos < file_size {
        let mut tries = 0;
        let mut resp = loop {
            let ret: anyhow::Result<_> = client
                .get(&download_url)
                // We already have timeout for each chunk.
                // FIXME: Use `Duration::MAX`.
                .timeout(Duration::from_secs(u64::MAX))
                .header(header::RANGE, format!("bytes={}-", pos))
                .send()
                .await
                .map_err(|err| err.into())
                .and_then(|resp| {
                    if resp.status() != StatusCode::PARTIAL_CONTENT {
                        anyhow::bail!("Not Partial Content response: {}", resp.status());
                    }
                    Ok(resp)
                });
            match ret {
                Ok(resp) => break resp,
                Err(err) => {
                    tries += 1;
                    log::error!(
                        "Error downloading file (try {}/{}): {}",
                        tries,
                        config.max_retry,
                        err,
                    );
                    if config.max_retry < tries {
                        return;
                    }
                    tokio::time::sleep(config.retry_delay).await;
                }
            }
        };

        loop {
            let chunk = match time::timeout(config.chunk_timeout, resp.chunk()).await {
                Err(_) => {
                    log::error!("Download stream timeout");
                    break;
                }
                Ok(Err(err)) => {
                    log::error!("Download stream error: {}", err);
                    break;
                }
                Ok(Ok(None)) => {
                    if pos != file_size {
                        log::error!("Download stream ends too early");
                    }
                    break;
                }
                Ok(Ok(Some(chunk))) => chunk,
            };

            pos += chunk.len() as u64;
            assert!(pos <= file_size);
            if tx.send(chunk).await.is_err() {
                log::debug!(
                    "Download stopped at {} bytes ({} bytes in total)",
                    pos,
                    file_size,
                );
                return;
            }
        }
    }

    assert_eq!(pos, file_size);
    log::debug!("Download finished ({} bytes)", file_size);
}

#[derive(Debug)]
struct DiskCache {
    dir: PathBuf,
    total_size: Arc<AtomicU64>,
    cache: SyncMutex<LruCache<ItemId, Arc<FileCache>>>,
    config: Config,
}

impl DiskCache {
    fn new(config: Config) -> io::Result<Self> {
        let disk_config = &config.disk_cache;
        assert!(disk_config.enable);
        assert!(disk_config.max_cached_file_size <= disk_config.max_total_size);

        let dir = disk_config.path.clone();
        std::fs::create_dir_all(&dir)?;
        log::info!("Disk file cache enabled at: {}", dir.display());
        Ok(Self {
            dir,
            total_size: Arc::new(0.into()),
            cache: SyncMutex::new(LruCache::new(disk_config.max_files)),
            config,
        })
    }

    fn get(&self, item_id: &ItemId) -> Option<Arc<FileCache>> {
        self.cache.lock().unwrap().get_mut(item_id).cloned()
    }

    fn try_alloc_and_fetch(
        &self,
        item_id: &ItemId,
        meta: &RemoteFileMeta,
        truncate_to: Option<(u64, SystemTime)>,
        onedrive: ManagedOnedrive,
        event_tx: mpsc::Sender<UpdateEvent>,
        client: reqwest::Client,
    ) -> io::Result<Option<Arc<FileCache>>> {
        let (file_size, download_truncate) = match truncate_to {
            None => (meta.size, None),
            Some((new_size, mtime)) => (new_size, Some((meta.size.min(new_size), mtime))),
        };

        if self.config.disk_cache.max_cached_file_size < file_size {
            return Ok(None);
        }

        let mut cache = self.cache.lock().unwrap();
        if let Some(state) = cache.get_mut(item_id) {
            return Ok(Some(state.clone()));
        }

        // Drop LRU until we have enough space.
        while self.config.disk_cache.max_cached_file_size
            < self.total_size.load(Ordering::Relaxed) + file_size
        {
            if cache.remove_lru().is_none() {
                // Cache is already empty.
                return Ok(None);
            }
        }

        let cache_file = tempfile::tempfile_in(&self.dir)?;
        cache_file.set_len(file_size)?;

        // The channel size doesn't really matter, since it's just for synchronization
        // between downloading and writing.
        let (chunk_tx, chunk_rx) = mpsc::channel(64);
        let (file, pos_tx) = FileCache::new(
            item_id.clone(),
            file_size,
            meta.c_tag.clone(),
            FileCacheStatus::Downloading {
                truncate: download_truncate,
            },
            cache_file.into(),
            &self.total_size,
        );
        cache.insert(item_id.clone(), file.clone());
        tokio::spawn(FileCache::write_to_cache_thread(
            file.clone(),
            chunk_rx,
            pos_tx,
            onedrive,
            client.clone(),
            event_tx,
            self.config.upload.clone(),
        ));
        tokio::spawn(download_thread(
            meta.size,
            meta.download_url.clone(),
            chunk_tx,
            client,
            self.config.download.clone(),
        ));
        Ok(Some(file))
    }

    async fn insert_empty(&self, item_id: ItemId, c_tag: Tag) -> Result<Arc<FileCache>> {
        let cache_file = tempfile::tempfile_in(&self.dir)?;
        let (file, old) = {
            let mut cache = self.cache.lock().unwrap();
            let (file, _) = FileCache::new(
                item_id.clone(),
                0,
                c_tag,
                FileCacheStatus::Available,
                cache_file.into(),
                &self.total_size,
            );
            let old = cache.insert(item_id, file.clone());
            (file, old)
        };
        if let Some(old) = old {
            old.state.lock().await.status = FileCacheStatus::Invalidated;
        }
        Ok(file)
    }

    async fn sync_items(&self, items: &[DriveItem]) {
        let mut outdated = Vec::new();
        {
            let mut cache = self.cache.lock().unwrap();
            for item in items {
                if item.folder.is_some() {
                    continue;
                }

                let id = item.id.clone().expect("Missing id");
                let file = match cache.get_mut(&id) {
                    Some(file) => file,
                    None => continue,
                };
                if item.deleted.is_some() {
                    log::debug!("Cached file {:?} is deleted", file.item_id);
                    outdated.push(cache.remove(&id).unwrap());
                    continue;
                }

                let c_tag = item.c_tag.clone().expect("Missing c_tag");
                let old_c_tag = file.c_tag.lock().unwrap();
                if *old_c_tag == c_tag {
                    log::debug!("Cached file {:?} is still up-to-date", *old_c_tag);
                } else {
                    log::debug!(
                        "Cached file {:?} is outdated, ctag: {:?} -> {:?}",
                        file.item_id,
                        *old_c_tag,
                        c_tag,
                    );
                    drop(old_c_tag);
                    outdated.push(cache.remove(&id).unwrap());
                }
            }
        }
        for file in outdated {
            file.state.lock().await.status = FileCacheStatus::Invalidated;
        }
    }
}

#[derive(Debug)]
struct FileCache {
    state: Mutex<FileCacheState>,
    item_id: ItemId,
    c_tag: SyncMutex<Tag>,
    cache_total_size: Weak<AtomicU64>,
}

#[derive(Debug)]
struct FileCacheState {
    status: FileCacheStatus,
    file_size: u64,
    available_size: watch::Receiver<u64>,
    cache_file: tokio::fs::File,
}

#[derive(Debug)]
enum FileCacheStatus {
    /// File is downloading.
    ///
    /// `truncate` is `Some(download_size, truncate_mtime)` if there is a pending truncation.
    Downloading { truncate: Option<(u64, SystemTime)> },
    /// Download failed.
    DownloadFailed,
    /// File is downloaded or created, and is synchronized with remote side.
    Available,
    /// File is downloaded or created, and is uploading or waiting for uploading.
    /// The parameter is used for mark-up of delayed flush.
    Dirty {
        lock_mtime: Instant,
        flush_tx: Option<oneshot::Sender<()>>,
        /// When closed, `true` indicates a successful upload, while `false` indicates still dirty.
        done_rx: watch::Receiver<bool>,
    },
    /// File is changed in remote side, local cache is invalidated.
    Invalidated,
}

impl FileCache {
    fn new(
        item_id: ItemId,
        file_size: u64,
        c_tag: Tag,
        status: FileCacheStatus,
        cache_file: tokio::fs::File,
        cache_total_size: &Arc<AtomicU64>,
    ) -> (Arc<Self>, watch::Sender<u64>) {
        let (pos_tx, pos_rx) = watch::channel(0);
        cache_total_size.fetch_add(file_size, Ordering::Relaxed);
        let this = Arc::new(Self {
            state: Mutex::new(FileCacheState {
                status,
                file_size,
                available_size: pos_rx,
                cache_file,
            }),
            item_id,
            c_tag: SyncMutex::new(c_tag),
            cache_total_size: Arc::downgrade(cache_total_size),
        });
        (this, pos_tx)
    }

    async fn write_to_cache_thread(
        this: Arc<FileCache>,
        mut chunk_rx: mpsc::Receiver<Bytes>,
        pos_tx: watch::Sender<u64>,
        onedrive: ManagedOnedrive,
        client: reqwest::Client,
        event_tx: mpsc::Sender<UpdateEvent>,
        upload_config: UploadConfig,
    ) {
        let mut pos = 0u64;

        let complete = |mut guard: MutexGuard<'_, FileCacheState>, download_size: u64| {
            log::debug!(
                "Cache {:?} is fully available (downloaded {} bytes, total {} bytes)",
                this.item_id,
                download_size,
                guard.file_size,
            );

            match guard.status {
                FileCacheStatus::Downloading {
                    truncate: Some((_, mtime)),
                } => {
                    log::debug!(
                        "Pending upload for truncated file {:?}, size: {}, mtime: {}",
                        this.item_id,
                        guard.file_size,
                        humantime::format_rfc3339_seconds(mtime),
                    );
                    this.queue_upload(
                        &mut guard,
                        mtime,
                        onedrive.clone(),
                        client.clone(),
                        event_tx,
                        upload_config,
                    );
                }
                FileCacheStatus::Downloading { truncate: None } => {
                    guard.status = FileCacheStatus::Available;
                }
                _ => unreachable!(),
            }
        };

        while let Some(mut chunk) = chunk_rx.recv().await {
            let mut guard = this.state.lock().await;
            let download_size = match guard.status {
                FileCacheStatus::Downloading {
                    truncate: Some((download_size, _)),
                } => download_size,
                // If there is no pending set_len, download should be aborted when removed from cache.
                FileCacheStatus::Downloading { truncate: None }
                    if Arc::strong_count(&this) != 1 =>
                {
                    guard.file_size
                }
                FileCacheStatus::Downloading { .. } | FileCacheStatus::Invalidated => return,
                FileCacheStatus::DownloadFailed { .. }
                | FileCacheStatus::Available
                | FileCacheStatus::Dirty { .. } => unreachable!(),
            };
            assert!(download_size <= guard.file_size);

            // Truncate extra data if `set_len` is called.
            let rest_len = download_size.saturating_sub(pos);
            if rest_len < chunk.len() as u64 {
                chunk.truncate(rest_len as usize);
            }

            if !chunk.is_empty() {
                guard.cache_file.seek(SeekFrom::Start(pos)).await.unwrap();
                guard.cache_file.write_all(&chunk).await.unwrap();
                pos += chunk.len() as u64;
            }
            log::trace!(
                "Write {} bytes to cache {:?}, current pos: {}, total need download: {}, file size: {}",
                chunk.len(),
                this.item_id,
                pos,
                download_size,
                guard.file_size,
            );

            if pos < download_size {
                // We are holding `state`.
                pos_tx.send(pos).unwrap();
            } else {
                // We are holding `state`.
                // The file size may be larger then download size due to set_len.
                // Space after data written is already zero as expected.
                pos_tx.send(guard.file_size).unwrap();

                complete(guard, download_size);
                return;
            }
        }

        let mut guard = this.state.lock().await;
        let download_size = match guard.status {
            FileCacheStatus::Downloading { truncate } => {
                truncate.map(|(sz, _)| sz).unwrap_or(guard.file_size)
            }
            FileCacheStatus::Invalidated => return,
            FileCacheStatus::DownloadFailed { .. }
            | FileCacheStatus::Available
            | FileCacheStatus::Dirty { .. } => unreachable!(),
        };

        if pos < download_size {
            log::error!(
                "Download failed of {:?}, got {}/{}",
                this.item_id,
                pos,
                download_size,
            );
            guard.status = FileCacheStatus::DownloadFailed;
        } else {
            // File is set to a larger length than remote side.
            complete(guard, download_size);
        }
    }

    async fn read(this: &Arc<Self>, offset: u64, size: usize) -> Result<Bytes> {
        let mut guard = this.state.lock().await;
        let file_size = guard.file_size;
        if file_size <= offset || size == 0 {
            return Ok(Bytes::new());
        }
        let end = offset + size as u64;

        match guard.status {
            FileCacheStatus::Available | FileCacheStatus::Dirty { .. } => {}
            FileCacheStatus::Invalidated => return Err(Error::Invalidated),
            FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
            FileCacheStatus::Downloading { .. } if end <= *guard.available_size.borrow() => {}
            FileCacheStatus::Downloading { .. } => {
                let mut rx = guard.available_size.clone();
                drop(guard);
                // Wait until finished or enough bytes are available.
                while rx.changed().await.is_ok() && *rx.borrow() < end {}

                guard = this.state.lock().await;
                match guard.status {
                    FileCacheStatus::Invalidated => return Err(Error::Invalidated),
                    FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
                    FileCacheStatus::Available
                    | FileCacheStatus::Dirty { .. }
                    | FileCacheStatus::Downloading { .. } => {}
                }
            }
        }

        // File size should be retrive after waiting since it may change.
        let end = end.min(guard.file_size);

        let mut buf = vec![0u8; (end - offset) as usize];
        guard
            .cache_file
            .seek(SeekFrom::Start(offset))
            .await
            .unwrap();
        guard.cache_file.read_exact(&mut buf).await.unwrap();
        Ok(buf.into())
    }

    async fn write(
        this: &Arc<Self>,
        offset: u64,
        data: &[u8],
        event_tx: mpsc::Sender<UpdateEvent>,
        onedrive: ManagedOnedrive,
        unlimit_client: reqwest::Client,
        config: UploadConfig,
    ) -> Result<UpdatedFileAttr> {
        let mut guard = this.state.lock().await;
        if config.max_size < offset + data.len() as u64 {
            return Err(Error::FileTooLarge);
        }
        match guard.status {
            FileCacheStatus::Available | FileCacheStatus::Dirty { .. } => {}
            FileCacheStatus::Invalidated => return Err(Error::Invalidated),
            FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
            FileCacheStatus::Downloading { .. } => {
                let mut rx = guard.available_size.clone();
                drop(guard);
                // Wait until finished.
                while rx.changed().await.is_ok() {}
                guard = this.state.lock().await;
            }
        }

        let mtime = SystemTime::now();
        match guard.status {
            FileCacheStatus::Invalidated => return Err(Error::Invalidated),
            FileCacheStatus::DownloadFailed => return Err(Error::DownloadFailed),
            FileCacheStatus::Downloading { .. } => unreachable!(),
            FileCacheStatus::Dirty { .. } | FileCacheStatus::Available => {
                this.queue_upload(
                    &mut guard,
                    mtime,
                    onedrive,
                    unlimit_client.clone(),
                    event_tx.clone(),
                    config,
                );
            }
        }

        guard
            .cache_file
            .seek(SeekFrom::Start(offset))
            .await
            .unwrap();
        guard.cache_file.write_all(data).await.unwrap();

        let new_size = guard.file_size.max(offset + data.len() as u64);
        if guard.file_size < new_size {
            if let Some(total) = this.cache_total_size.upgrade() {
                total.fetch_add(new_size - guard.file_size, Ordering::Relaxed);
            }
        }
        log::debug!(
            "Cached file {:?} is dirty, size: {} -> {}",
            this.item_id,
            guard.file_size,
            new_size,
        );
        guard.file_size = new_size;

        Ok(UpdatedFileAttr {
            item_id: this.item_id.clone(),
            size: new_size,
            mtime,
            // CTag is currently unknown and will be filled after a successful upload.
            c_tag: Tag(String::new()),
        })
    }

    fn queue_upload(
        self: &Arc<Self>,
        guard: &mut MutexGuard<'_, FileCacheState>,
        mtime: SystemTime,
        onedrive: ManagedOnedrive,
        client: reqwest::Client,
        event_tx: mpsc::Sender<UpdateEvent>,
        config: UploadConfig,
    ) {
        const UPLOAD_PART_SIZE: usize = 10 << 20;
        static_assertions::const_assert!(
            UPLOAD_PART_SIZE <= onedrive_api::UploadSession::MAX_PART_SIZE,
        );

        let (flush_tx, flush_rx) = oneshot::channel();
        let (done_tx, done_rx) = watch::channel(false);
        let init_lock_mtime = Instant::now();
        guard.status = FileCacheStatus::Dirty {
            lock_mtime: init_lock_mtime,
            flush_tx: Some(flush_tx),
            done_rx,
        };

        let this = self.clone();
        tokio::spawn(async move {
            let _ = time::timeout(config.flush_delay, flush_rx).await;

            let is_up_to_date = |status: &FileCacheStatus| matches!(status, FileCacheStatus::Dirty { lock_mtime, .. } if *lock_mtime == init_lock_mtime);

            loop {
                // Check not changed since last lock.
                let file_size = {
                    let guard = this.state.lock().await;
                    if !is_up_to_date(&guard.status) {
                        return;
                    }
                    guard.file_size
                };

                // Create upload session.
                log::info!("Uploading {:?} ({} B)", this.item_id, file_size);
                let mut initial = DriveItem::default();
                initial.file_system_info = Some(Box::new(serde_json::json!({
                    "lastModifiedDateTime": humantime::format_rfc3339_seconds(mtime).to_string(),
                })));
                let sess = match onedrive
                    .get()
                    .await
                    .new_upload_session_with_initial_option(
                        ItemLocation::from_id(&this.item_id),
                        &initial,
                        DriveItemPutOption::new().conflict_behavior(ConflictBehavior::Replace),
                    )
                    .await
                {
                    Ok((sess, _)) => sess,
                    Err(err) => {
                        log::error!(
                            "Failed to create upload session of {:?} ({} B), retrying: {}",
                            this.item_id,
                            file_size,
                            err,
                        );
                        // Retry
                        time::sleep(config.retry_delay).await;
                        continue;
                    }
                };

                // Upload parts.
                let mut pos = 0u64;
                let mut buf = vec![0u8; UPLOAD_PART_SIZE];
                let item = loop {
                    let end = file_size.min(pos + UPLOAD_PART_SIZE as u64);
                    let len = (end - pos) as usize;
                    {
                        let mut guard = this.state.lock().await;
                        if !is_up_to_date(&guard.status) {
                            log::debug!("Upload session of {:?} outdates", this.item_id);
                            if let Err(err) = sess.delete(onedrive.get().await.client()).await {
                                log::error!(
                                    "Failed to delete outdated upload session of {:?}: {}",
                                    this.item_id,
                                    err,
                                );
                            }
                            return;
                        }
                        assert_eq!(file_size, guard.file_size, "Truncation restarts uploading");
                        guard.cache_file.seek(SeekFrom::Start(pos)).await.unwrap();
                        guard.cache_file.read_exact(&mut buf[..len]).await.unwrap();
                    }

                    match sess
                        .upload_part(buf[..len].to_owned(), pos..end, file_size, &client)
                        .await
                    {
                        Ok(None) => {
                            assert_ne!(end, file_size);
                            log::debug!(
                                "Uploaded part {}..{}/{} of file {:?}",
                                pos,
                                end,
                                file_size,
                                this.item_id,
                            );
                            pos = end;
                        }
                        Ok(Some(item)) => {
                            assert_eq!(end, file_size);
                            break item;
                        }
                        Err(err) => {
                            log::error!(
                                "Failed to upload part {}..{}/{} of file {:?}, retrying: {}",
                                pos,
                                end,
                                file_size,
                                this.item_id,
                                err,
                            );
                            // Retry
                            time::sleep(config.retry_delay).await;
                            continue;
                        }
                    }
                };

                let attr = super::InodeAttr::parse_item(&item).expect("Invalid attrs");
                assert_eq!(item.id.as_ref(), Some(&this.item_id));
                assert_eq!(attr.size, file_size);
                let c_tag = item.c_tag.expect("Missing c_tag");
                log::info!(
                    "Uploaded {:?} ({} B), new c_tag: {:?}",
                    this.item_id,
                    file_size,
                    c_tag,
                );

                {
                    let mut guard = this.state.lock().await;
                    match guard.status {
                        FileCacheStatus::Downloading { .. } => unreachable!(),
                        FileCacheStatus::Dirty { lock_mtime, .. }
                            if lock_mtime == init_lock_mtime =>
                        {
                            guard.status = FileCacheStatus::Available;
                        }
                        FileCacheStatus::Invalidated => {
                            log::warn!(
                                "Cache invalidated during the upload of {:?}, maybe both changed? Suppress update event",
                                this.item_id,
                            );
                            return;
                        }
                        // Race another upload.
                        _ => {
                            log::debug!("Racing upload? Suppress update event");
                            return;
                        }
                    }
                    *this.c_tag.lock().unwrap() = c_tag.clone();
                    log::debug!("New c_tag of {:?} saved", this.item_id);
                }

                let _ = event_tx
                    .send(UpdateEvent::UpdateFile(UpdatedFileAttr {
                        item_id: this.item_id.clone(),
                        size: attr.size,
                        mtime: attr.mtime,
                        c_tag,
                    }))
                    .await;
                let _ = done_tx.send(true);

                return;
            }
        });
    }
}

impl Drop for FileCache {
    fn drop(&mut self) {
        if let Some(arc) = self.cache_total_size.upgrade() {
            arc.fetch_sub(self.state.get_mut().file_size, Ordering::Relaxed);
        }
    }
}
