use crate::{
    config::de_duration_sec,
    vfs::{Error, Result},
};
use bytes::Bytes;
use lru_cache::LruCache;
use onedrive_api::{ItemId, ItemLocation, OneDrive};
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
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, watch, Mutex},
};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    stream_buffer_chunks: usize,
    #[serde(flatten)]
    retry: RetryConfig,
    disk_cache: DiskCacheConfig,
}

#[derive(Debug, Deserialize, Clone)]
struct RetryConfig {
    download_max_retry: usize,
    #[serde(deserialize_with = "de_duration_sec")]
    download_retry_delay: Duration,
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

fn default_disk_cache_dir() -> PathBuf {
    std::env::temp_dir().join("onedrive_fuse-cache")
}

pub struct FilePool {
    handles: Slab<File>,
    disk_cache: Option<DiskCache>,
    config: Config,
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
            }
            (file_size, download_url)
        } else {
            Self::fetch_meta(item_id, onedrive).await?
        };

        log::debug!("Streaming file {:?}, url: {}", item_id, download_url);
        let state = FileStreamState::fetch(file_size, download_url, client.clone(), &self.config);
        Ok(File::Streaming {
            file_size,
            state: Arc::new(Mutex::new(state)),
        })
    }

    // Support read mode only.
    pub async fn open(
        &self,
        item_id: &ItemId,
        onedrive: &OneDrive,
        client: &reqwest::Client,
    ) -> Result<u64> {
        let file = self.open_inner(item_id, onedrive, client).await?;
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
}

#[derive(Debug, Clone)]
enum File {
    Streaming {
        file_size: u64,
        state: Arc<Mutex<FileStreamState>>,
    },
    Cached(Arc<FileCacheState>),
}

impl File {
    fn file_size(&self) -> u64 {
        match self {
            Self::Streaming { file_size, .. } => *file_size,
            Self::Cached(state) => state.file_size,
        }
    }

    async fn read(&self, offset: u64, mut size: usize) -> Result<impl AsRef<[u8]>> {
        let file_size = self.file_size();
        if file_size <= offset || size == 0 {
            return Ok(Bytes::new());
        }
        if let Ok(rest) = usize::try_from(file_size - offset) {
            size = size.min(rest);
        }

        match self {
            Self::Streaming { state, .. } => state.lock().await.read(file_size, offset, size).await,
            Self::Cached(state) => state.read(offset, size).await,
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
        config: &Config,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.stream_buffer_chunks);
        tokio::spawn(download_thread(
            file_size,
            download_url,
            tx,
            client,
            config.retry.clone(),
        ));
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
    mut tx: mpsc::Sender<Bytes>,
    client: reqwest::Client,
    retry: RetryConfig,
) -> std::result::Result<(), ()> {
    let mut pos = 0u64;

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
                        retry.download_max_retry,
                        err,
                    );
                    if retry.download_max_retry < tries {
                        return Err(());
                    }
                    tokio::time::delay_for(retry.download_retry_delay).await;
                }
            }
        };

        while let Some(chunk) = resp.chunk().await.ok().flatten() {
            pos += chunk.len() as u64;
            tx.send(chunk).await.map_err(|_| ())?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct DiskCache {
    dir: PathBuf,
    total_size: Arc<AtomicU64>,
    cache: SyncMutex<LruCache<ItemId, Arc<FileCacheState>>>,
    config: Config,
}

impl DiskCache {
    fn new(config: Config) -> io::Result<Self> {
        let disk_config = &config.disk_cache;
        assert!(disk_config.enable);
        assert!(disk_config.max_cached_file_size <= disk_config.max_total_size);

        let dir = disk_config.path.clone();
        std::fs::create_dir_all(&dir)?;
        log::debug!("Enabled file cache at: {}", dir.display());
        Ok(Self {
            dir,
            total_size: Arc::new(0.into()),
            cache: SyncMutex::new(LruCache::new(disk_config.max_files)),
            config,
        })
    }

    fn get(&self, item_id: &ItemId) -> Option<Arc<FileCacheState>> {
        self.cache.lock().unwrap().get_mut(item_id).cloned()
    }

    fn try_alloc_and_fetch(
        &self,
        item_id: &ItemId,
        file_size: u64,
        download_url: &str,
        client: &reqwest::Client,
    ) -> io::Result<Option<Arc<FileCacheState>>> {
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
            file_size,
            download_url.to_owned(),
            cache_file.into(),
            &self.total_size,
            client.clone(),
            self.config.retry.clone(),
        );
        cache.insert(item_id.clone(), state.clone());
        Ok(Some(state))
    }
}

#[derive(Debug)]
struct FileCacheState {
    file_size: u64,
    cache_total_size: Weak<AtomicU64>,
    available_size: watch::Receiver<u64>,
    cache_file: Mutex<tokio::fs::File>,
}

impl FileCacheState {
    fn fetch(
        file_size: u64,
        download_url: String,
        cache_file: tokio::fs::File,
        cache_total_size: &Arc<AtomicU64>,
        client: reqwest::Client,
        retry: RetryConfig,
    ) -> Arc<Self> {
        let (pos_tx, pos_rx) = watch::channel(0);
        let state = Arc::new(Self {
            file_size,
            cache_total_size: Arc::downgrade(&cache_total_size),
            available_size: pos_rx,
            cache_file: Mutex::new(cache_file),
        });
        cache_total_size.fetch_add(file_size, Ordering::Relaxed);

        // The channel size doesn't really matter, since it's just for synchronization
        // between downloading and writing.
        let (chunk_tx, chunk_rx) = mpsc::channel(64);
        tokio::spawn(download_thread(
            file_size,
            download_url,
            chunk_tx,
            client,
            retry,
        ));
        tokio::spawn(Self::write_to_cache_thread(
            Arc::downgrade(&state),
            chunk_rx,
            pos_tx,
        ));
        state
    }

    async fn write_to_cache_thread(
        state: Weak<FileCacheState>,
        mut chunk_rx: mpsc::Receiver<Bytes>,
        pos_tx: watch::Sender<u64>,
    ) -> std::result::Result<(), ()> {
        let mut pos = 0u64;
        while let Some(chunk) = chunk_rx.recv().await {
            let state = state.upgrade().ok_or(())?;
            async {
                let mut file = state.cache_file.lock().await;
                file.seek(SeekFrom::Start(pos)).await?;
                file.write_all(&chunk).await?;
                io::Result::Ok(())
            }
            .await
            .map_err(|err| log::error!("Failed to seek and write: {}", err))?;

            pos += chunk.len() as u64;
            // We are holding `state`.
            pos_tx.broadcast(pos).unwrap();
        }
        Ok(())
    }

    /// `offset` and `size` must be already clamped.
    async fn read(&self, offset: u64, size: usize) -> Result<Bytes> {
        let end = offset + size as u64;
        // Wait until our bytes are available.
        if *self.available_size.borrow() < end {
            let mut rx = self.available_size.clone();
            while rx.recv().await.map_or(false, |pos| pos < end) {}
            let pos = *rx.borrow();
            if pos < end {
                return Err(Error::UnexpectedEndOfDownload {
                    current_pos: pos,
                    file_size: self.file_size,
                });
            }
        }

        async {
            let mut buf = vec![0u8; size];
            let mut file = self.cache_file.lock().await;
            file.seek(SeekFrom::Start(offset)).await?;
            file.read_exact(&mut buf).await?;
            io::Result::Ok(buf.into())
        }
        .await
        .map_err(|err| err.into())
    }
}

impl Drop for FileCacheState {
    fn drop(&mut self) {
        if let Some(arc) = self.cache_total_size.upgrade() {
            arc.fetch_sub(self.file_size, Ordering::Relaxed);
        }
    }
}
