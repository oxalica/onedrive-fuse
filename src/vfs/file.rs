use crate::vfs::{Error, Result};
use bytes::Bytes;
use onedrive_api::{ItemId, ItemLocation, OneDrive};
use serde::Deserialize;
use sharded_slab::Slab;
use std::{convert::TryFrom as _, sync::Arc};
use tokio::sync::{mpsc, Mutex};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    stream_buffer_chunks: usize,
}

pub struct FilePool {
    handles: Slab<Arc<File>>,
    config: Config,
}

impl FilePool {
    pub fn new(config: Config) -> Self {
        Self {
            handles: Slab::new(),
            config,
        }
    }

    fn key_to_fh(key: usize) -> u64 {
        u64::try_from(key).unwrap()
    }

    fn fh_to_key(fh: u64) -> usize {
        usize::try_from(fh).unwrap()
    }

    // Support read mode only.
    pub async fn open(
        &self,
        item_id: &ItemId,
        onedrive: &OneDrive,
        client: &reqwest::Client,
    ) -> Result<u64> {
        // `download_url` is available without `$select`.
        let item = onedrive.get_item(ItemLocation::from_id(item_id)).await?;
        let file_size = item.size.unwrap() as u64;
        let download_url = item.download_url.unwrap();
        log::debug!("download: item_id={:?} url={}", item_id, download_url);

        let file = Arc::new(File::Streaming {
            file_size,
            state: Mutex::new(FileStreamState::run(
                download_url,
                client.clone(),
                &self.config,
            )),
        });

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

#[derive(Debug)]
enum File {
    Streaming {
        file_size: u64,
        state: Mutex<FileStreamState>,
    },
}

impl File {
    fn file_size(&self) -> u64 {
        match self {
            Self::Streaming { file_size, .. } => *file_size,
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
    fn run(download_url: String, client: reqwest::Client, config: &Config) -> Self {
        let (tx, rx) = mpsc::channel(config.stream_buffer_chunks);
        tokio::spawn(download_thread(download_url, tx, client));
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
    download_url: String,
    mut tx: mpsc::Sender<Bytes>,
    client: reqwest::Client,
) {
    let mut resp = match client
        .get(&download_url)
        .send()
        .await
        .and_then(|resp| resp.error_for_status())
    {
        Ok(resp) => resp,
        Err(err) => {
            log::error!("Failed to download file: {}", err);
            return;
        }
    };

    while let Some(chunk) = resp.chunk().await.ok().flatten() {
        if tx.send(chunk).await.is_err() {
            return;
        }
    }
}
