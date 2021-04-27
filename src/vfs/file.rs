use crate::{
    config::de_duration_sec,
    login::ManagedOnedrive,
    vfs::{Error, Result},
};
use bytes::Bytes;
use lru_cache::LruCache;
use onedrive_api::{resource::DriveItem, ItemId, ItemLocation, OneDrive};
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
    sync::{mpsc, watch, Mutex, MutexGuard},
    time,
};

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

fn default_disk_cache_dir() -> PathBuf {
    std::env::temp_dir().join("onedrive_fuse-cache")
}

pub struct FilePool {
    handles: Slab<File>,
    disk_cache: Option<DiskCache>,
    config: Config,
}

#[derive(Debug, Clone)]
pub struct UpdatedFileAttr {
    pub item_id: ItemId,
    pub size: u64,
    pub mtime: SystemTime,
}

impl FilePool {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        Ok(Self {
            handles: Slab::new(),
            disk_cache: if config.disk_cache.enable {
                Some(DiskCache::new(config.clone())?)
            } else {
                None
            },
            config,
        })
    }

    fn key_to_fh(key: usize) -> u64 {
        u64::try_from(key).unwrap()
    }

    fn fh_to_key(fh: u64) -> usize {
        usize::try_from(fh).unwrap()
    }

    // Fetch file size and download URL.
    async fn fetch_meta(item_id: &ItemId, onedrive: &OneDrive) -> Result<(u64, String)> {
        // `download_url` is available without `$select`.
        let item = onedrive.get_item(ItemLocation::from_id(item_id)).await?;
        let file_size = item.size.unwrap() as u64;
        let download_url = item.download_url.unwrap();
        Ok((file_size, download_url))
    }

    async fn open_inner(
        &self,
        item_id: &ItemId,
        write_mode: bool,
        onedrive: &OneDrive,
        client: &reqwest::Client,
    ) -> Result<File> {
        let (file_size, download_url) = if let Some(cache) = &self.disk_cache {
            if let Some(state) = cache.get(item_id) {
                log::debug!("File already cached: {:?}", item_id);
                return Ok(File::Cached(state));
            }

            let (file_size, download_url) = Self::fetch_meta(item_id, onedrive).await?;
            if let Some(state) =
                cache.try_alloc_and_fetch(item_id, file_size, &download_url, client)?
            {
                log::debug!("Caching file {:?}, url: {}", item_id, download_url);
                return Ok(File::Cached(state));
            } else if write_mode {
                return Err(Error::FileTooLarge);
            }

            (file_size, download_url)
        } else if write_mode {
            return Err(Error::WriteWithoutCache);
        } else {
            Self::fetch_meta(item_id, onedrive).await?
        };

        log::debug!("Streaming file {:?}, url: {}", item_id, download_url);
        let state = FileStreamState::fetch(
            file_size,
            download_url,
            client.clone(),
            self.config.download.clone(),
        );
        Ok(File::Streaming {
            file_size,
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub async fn open(
        &self,
        item_id: &ItemId,
        write_mode: bool,
        onedrive: &OneDrive,
        client: &reqwest::Client,
    ) -> Result<u64> {
        let file = self
            .open_inner(item_id, write_mode, onedrive, client)
            .await?;
        let key = self.handles.insert(file).expect("Pool is full");
        Ok(Self::key_to_fh(key))
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
        file.read(offset, size).await
    }

    /// Write to cached file. Returns item id and file size after the write.
    pub async fn write(
        &self,
        fh: u64,
        offset: u64,
        data: &[u8],
        onedrive: ManagedOnedrive,
    ) -> Result<UpdatedFileAttr> {
        let file = self
            .handles
            .get(Self::fh_to_key(fh))
            .ok_or(Error::InvalidHandle(fh))?
            .clone();
        file.write(offset, data, onedrive, self.config.upload.clone())
            .await
    }

    pub async fn sync_items(&self, items: &[DriveItem]) {
        if let Some(cache) = &self.disk_cache {
            cache
                .invalidate(
                    items
                        .iter()
                        .filter(|item| item.folder.is_none())
                        .map(|item| item.id.as_ref().unwrap()),
                )
                .await;
        }
    }
}

#[derive(Debug, Clone)]
enum File {
    Streaming {
        file_size: u64,
        state: Arc<Mutex<FileStreamState>>,
    },
    Cached(Arc<Mutex<FileCacheState>>),
}

impl File {
    async fn read(&self, offset: u64, size: usize) -> Result<impl AsRef<[u8]>> {
        match self {
            Self::Streaming { file_size, state } => {
                state.lock().await.read(*file_size, offset, size).await
            }
            Self::Cached(state) => FileCacheState::read(&*state, offset, size).await,
        }
    }

    async fn write(
        &self,
        offset: u64,
        data: &[u8],
        onedrive: ManagedOnedrive,
        upload_config: UploadConfig,
    ) -> Result<UpdatedFileAttr> {
        match self {
            Self::Streaming { .. } => panic!("Cannot stream in write mode"),
            Self::Cached(state) => {
                FileCacheState::write(&*state, offset, data, onedrive, upload_config).await
            }
        }
    }
}

#[derive(Debug)]
struct FileStreamState {
    current_pos: u64,
    buffer: Option<Bytes>,
    rx: mpsc::Receiver<Bytes>,
}

impl FileStreamState {
    fn fetch(
        file_size: u64,
        download_url: String,
        client: reqwest::Client,
        config: DownloadConfig,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.stream_buffer_chunks);
        tokio::spawn(download_thread(file_size, download_url, tx, client, config));
        Self {
            current_pos: 0,
            buffer: None,
            rx,
        }
    }

    /// `offset` and `size` must be already clamped.
    async fn read(&mut self, file_size: u64, offset: u64, size: usize) -> Result<Bytes> {
        if offset != self.current_pos {
            return Err(Error::NonsequentialRead {
                current_pos: self.current_pos,
                try_offset: offset,
            });
        }

        let mut ret_buf = Vec::with_capacity(size);
        loop {
            let chunk = match self.buffer.take() {
                Some(chunk) => chunk,
                None => match self.rx.recv().await {
                    Some(chunk) => chunk,
                    None => break,
                },
            };

            let buf_rest_len = ret_buf.capacity() - ret_buf.len();
            if buf_rest_len < chunk.len() {
                self.buffer = Some(chunk.slice(buf_rest_len..));
                ret_buf.extend_from_slice(&chunk[..buf_rest_len]);
                break;
            } else {
                ret_buf.extend_from_slice(&chunk);
                if ret_buf.len() == ret_buf.capacity() {
                    break;
                }
            }
        }

        self.current_pos += ret_buf.len() as u64;

        if ret_buf.len() == size {
            Ok(ret_buf.into())
        } else {
            Err(Error::UnexpectedEndOfDownload {
                current_pos: self.current_pos,
                file_size,
            })
        }
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

        while let Some(chunk) = resp.chunk().await.ok().flatten() {
            pos += chunk.len() as u64;
            assert!(pos <= file_size);
            if tx.send(chunk).await.is_err() {
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
    cache: SyncMutex<LruCache<ItemId, FileCacheStateSlot>>,
    config: Config,
}

type FileCacheStateSlot = Arc<Mutex<FileCacheState>>;

impl DiskCache {
    fn new(config: Config) -> io::Result<Self> {
        let disk_config = &config.disk_cache;
        assert!(disk_config.enable);
        assert!(disk_config.max_cached_file_size <= disk_config.max_total_size);

        let dir = disk_config.path.clone();
        std::fs::create_dir_all(&dir)?;
        log::debug!("Disk file cache enabled at: {}", dir.display());
        Ok(Self {
            dir,
            total_size: Arc::new(0.into()),
            cache: SyncMutex::new(LruCache::new(disk_config.max_files)),
            config,
        })
    }

    fn get(&self, item_id: &ItemId) -> Option<FileCacheStateSlot> {
        self.cache.lock().unwrap().get_mut(item_id).cloned()
    }

    fn try_alloc_and_fetch(
        &self,
        item_id: &ItemId,
        file_size: u64,
        download_url: &str,
        client: &reqwest::Client,
    ) -> io::Result<Option<FileCacheStateSlot>> {
        if self.config.disk_cache.max_cached_file_size < file_size {
            return Ok(None);
        }

        let mut cache = self.cache.lock().unwrap();
        if let Some(state) = cache.get_mut(&item_id) {
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
        let state = FileCacheState::fetch(
            item_id.clone(),
            file_size,
            download_url.to_owned(),
            cache_file.into(),
            &self.total_size,
            client.clone(),
            self.config.download.clone(),
        );
        cache.insert(item_id.clone(), state.clone());
        Ok(Some(state))
    }

    async fn invalidate<'a>(&self, item_ids: impl IntoIterator<Item = &'a ItemId>) {
        let mut invalidated = Vec::new();
        {
            let mut cache = self.cache.lock().unwrap();
            for item_id in item_ids {
                invalidated.extend(cache.remove(item_id));
            }
        }
        for cache in invalidated {
            cache.lock().await.status = FileCacheStatus::Invalidated;
        }
    }
}

#[derive(Debug)]
struct FileCacheState {
    status: FileCacheStatus,
    item_id: ItemId,
    file_size: u64,
    available_size: watch::Receiver<u64>,
    cache_file: tokio::fs::File,

    cache_total_size: Weak<AtomicU64>,
}

#[derive(Debug)]
enum FileCacheStatus {
    /// File is downloading.
    Downloading,
    /// File is downloaded or created, and is synchronized with remote side.
    Available,
    /// File is downloaded or created, and is uploading or waiting for uploading.
    Dirty(Instant),
    /// File is changed in remote side, local cache is invalidated.
    Invalidated,
}

impl FileCacheState {
    fn fetch(
        item_id: ItemId,
        file_size: u64,
        download_url: String,
        cache_file: tokio::fs::File,
        cache_total_size: &Arc<AtomicU64>,
        client: reqwest::Client,
        config: DownloadConfig,
    ) -> Arc<Mutex<Self>> {
        let (pos_tx, pos_rx) = watch::channel(0);
        let state = Arc::new(Mutex::new(Self {
            status: FileCacheStatus::Downloading,
            item_id,
            file_size,
            available_size: pos_rx,
            cache_file,
            cache_total_size: Arc::downgrade(&cache_total_size),
        }));
        cache_total_size.fetch_add(file_size, Ordering::Relaxed);

        // The channel size doesn't really matter, since it's just for synchronization
        // between downloading and writing.
        let (chunk_tx, chunk_rx) = mpsc::channel(64);
        tokio::spawn(download_thread(
            file_size,
            download_url,
            chunk_tx,
            client,
            config,
        ));
        tokio::spawn(Self::write_to_cache_thread(
            Arc::downgrade(&state),
            chunk_rx,
            pos_tx,
        ));
        state
    }

    async fn write_to_cache_thread(
        state: Weak<Mutex<FileCacheState>>,
        mut chunk_rx: mpsc::Receiver<Bytes>,
        pos_tx: watch::Sender<u64>,
    ) {
        let mut pos = 0u64;
        while let Some(chunk) = chunk_rx.recv().await {
            let state = match state.upgrade() {
                Some(state) => state,
                None => return,
            };
            let mut guard = state.lock().await;
            guard.cache_file.seek(SeekFrom::Start(pos)).await.unwrap();
            guard.cache_file.write_all(&chunk).await.unwrap();

            pos += chunk.len() as u64;
            log::trace!(
                "Write {} bytes to cache, current pos: {}, total size: {}",
                chunk.len(),
                pos,
                guard.file_size,
            );

            // We are holding `state`.
            pos_tx.send(pos).unwrap();

            if pos == guard.file_size {
                log::debug!("Cache available ({} bytes)", guard.file_size);
                assert!(matches!(guard.status, FileCacheStatus::Downloading));
                guard.status = FileCacheStatus::Available;
                return;
            }
        }
    }

    /// Wait until download completion, or at least `end` bytes in total are available.
    async fn wait_bytes_available(
        this: &FileCacheStateSlot,
        end: u64,
    ) -> Result<MutexGuard<'_, FileCacheState>> {
        let mut rx = {
            let guard = this.lock().await;
            match guard.status {
                FileCacheStatus::Available | FileCacheStatus::Dirty(_) => return Ok(guard),
                FileCacheStatus::Invalidated => return Err(Error::Invalidated),
                FileCacheStatus::Downloading => {}
            }
            if end <= *guard.available_size.borrow() {
                return Ok(guard);
            }

            guard.available_size.clone()
        };

        while rx.changed().await.is_ok() && *rx.borrow() < end {}

        let guard = this.lock().await;
        match guard.status {
            FileCacheStatus::Available | FileCacheStatus::Dirty(_) => return Ok(guard),
            FileCacheStatus::Invalidated => return Err(Error::Invalidated),
            FileCacheStatus::Downloading => {
                let available = *rx.borrow();
                if available < end {
                    return Err(Error::UnexpectedEndOfDownload {
                        current_pos: available,
                        file_size: guard.file_size,
                    });
                }
                Ok(guard)
            }
        }
    }

    async fn read(this: &FileCacheStateSlot, offset: u64, size: usize) -> Result<Bytes> {
        let end = {
            let guard = this.lock().await;
            let file_size = guard.file_size;
            if file_size <= offset || size == 0 {
                return Ok(Bytes::new());
            }
            file_size.min(offset + size as u64)
        };

        let mut guard = Self::wait_bytes_available(this, end).await?;

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
        this: &FileCacheStateSlot,
        offset: u64,
        data: &[u8],
        onedrive: ManagedOnedrive,
        config: UploadConfig,
    ) -> Result<UpdatedFileAttr> {
        let file_size = this.lock().await.file_size;
        if config.max_size < offset + data.len() as u64 {
            return Err(Error::FileTooLarge);
        }

        let mut guard = Self::wait_bytes_available(this, file_size).await?;
        let mtime = Instant::now();
        let mtime_sys = SystemTime::now();
        match guard.status {
            FileCacheStatus::Downloading | FileCacheStatus::Invalidated => unreachable!(),
            FileCacheStatus::Dirty(_) | FileCacheStatus::Available => {
                guard.status = FileCacheStatus::Dirty(mtime);
                tokio::spawn(Self::upload_thread(this.clone(), onedrive, mtime, config));
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
            if let Some(total) = guard.cache_total_size.upgrade() {
                total.fetch_add(new_size - guard.file_size, Ordering::Relaxed);
            }
        }
        guard.file_size = new_size;
        Ok(UpdatedFileAttr {
            item_id: guard.item_id.clone(),
            size: new_size,
            mtime: mtime_sys,
        })
    }

    async fn upload_thread(
        this: Arc<Mutex<FileCacheState>>,
        onedrive: ManagedOnedrive,
        lock_mtime: Instant,
        config: UploadConfig,
    ) {
        time::sleep(config.flush_delay).await;

        loop {
            // Check not changed since last lock.
            let (item_id, data) = {
                let mut guard = this.lock().await;
                match guard.status {
                    FileCacheStatus::Dirty(mtime) if mtime == lock_mtime => {
                        let mut buf = vec![0u8; guard.file_size as usize];
                        guard.cache_file.seek(SeekFrom::Start(0)).await.unwrap();
                        guard.cache_file.read_exact(&mut buf).await.unwrap();
                        (guard.item_id.clone(), buf)
                    }
                    _ => return,
                }
            };
            let file_len = data.len();

            // Do upload.
            log::info!("Uploading {:?} ({} B)", item_id, file_len);
            match onedrive
                .get()
                .await
                .upload_small(ItemLocation::from_id(&item_id), data)
                .await
            {
                Ok(_) => {
                    log::info!("Uploaded {:?} ({} B)", item_id, file_len);
                    break;
                }
                Err(err) => {
                    log::error!("Failed to upload {:?} ({} B): {}", item_id, file_len, err);
                }
            }

            // Retry
            time::sleep(config.retry_delay).await;
        }

        let mut guard = this.lock().await;
        match guard.status {
            FileCacheStatus::Dirty(mtime) if mtime == lock_mtime => {
                guard.status = FileCacheStatus::Available;
            }
            _ => {}
        }
    }
}

impl Drop for FileCacheState {
    fn drop(&mut self) {
        if let Some(arc) = self.cache_total_size.upgrade() {
            arc.fetch_sub(self.file_size, Ordering::Relaxed);
        }
    }
}
