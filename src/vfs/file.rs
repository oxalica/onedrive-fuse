use crate::vfs::{Error, Result};
use bytes::Bytes;
use onedrive_api::{ItemId, ItemLocation, OneDrive};
use reqwest::{header, StatusCode};
use serde::Deserialize;
use sharded_slab::Slab;
use std::{convert::TryFrom as _, sync::Arc};
use tokio::sync::Mutex;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {}

pub struct FilePool {
    read_handles: Slab<Arc<FileRead>>,
}

impl FilePool {
    pub fn new(_config: Config) -> Self {
        Self {
            read_handles: Slab::new(),
        }
    }

    fn key_to_fh(key: usize) -> u64 {
        u64::try_from(key).unwrap()
    }

    fn fh_to_key(fh: u64) -> usize {
        usize::try_from(fh).unwrap()
    }

    // Support read mode only.
    pub async fn open(&self, item_id: &ItemId, onedrive: &OneDrive) -> Result<u64> {
        // `download_url` is available without `$select`.
        let item = onedrive.get_item(ItemLocation::from_id(item_id)).await?;
        let file_size = item.size.unwrap() as u64;
        let download_url = item.download_url.unwrap();
        log::debug!("download: item_id={:?} url={}", item_id, download_url);
        let file = Arc::new(FileRead {
            download_url,
            file_size,
            state: Default::default(),
        });
        let key = self.read_handles.insert(file).expect("Pool is full");
        Ok(Self::key_to_fh(key))
    }

    pub async fn close(&self, fh: u64) -> Result<()> {
        if self.read_handles.remove(Self::fh_to_key(fh)) {
            Ok(())
        } else {
            Err(Error::InvalidHandle(fh))
        }
    }

    pub async fn read(
        &self,
        fh: u64,
        offset: u64,
        size: usize,
        client: &reqwest::Client,
    ) -> Result<impl AsRef<[u8]>> {
        let file = self
            .read_handles
            .get(Self::fh_to_key(fh))
            .ok_or(Error::InvalidHandle(fh))?
            .clone();

        if file.file_size <= offset {
            return Ok(Bytes::new());
        }

        let mut state = file.state.lock().await;
        let data = state.read(&*file, offset, size, client).await?;
        // We are not at the end of file. It is an error for the stream ended too early.
        if data.is_empty() {
            return Err(Error::UnexpectedEndOfDownload {
                current_pos: state.current_pos,
            });
        }
        Ok(data)
    }
}

#[derive(Debug)]
struct FileRead {
    download_url: String,
    file_size: u64,
    state: Mutex<FileReadState>,
}

#[derive(Debug, Default)]
struct FileReadState {
    current_pos: u64,
    response: Option<reqwest::Response>,
    buffer: Option<Bytes>,
}

impl FileReadState {
    async fn read(
        &mut self,
        file: &FileRead,
        offset: u64,
        size: usize,
        client: &reqwest::Client,
    ) -> Result<Bytes> {
        if offset != self.current_pos {
            return Err(Error::NonsequentialRead {
                current_pos: self.current_pos,
                try_offset: offset,
            });
        }

        if self.response.is_none() {
            let resp = client
                .get(&file.download_url)
                .header(header::RANGE, format!("bytes={}-", self.current_pos))
                .send()
                .await?;
            let check_range_response = || {
                if resp.status() != StatusCode::PARTIAL_CONTENT {
                    return None;
                }
                let val = resp.headers().get(header::CONTENT_RANGE)?.to_str().ok()?;
                if !val.starts_with(&format!("bytes {}-", self.current_pos)) {
                    return None;
                }
                Some(())
            };
            if check_range_response().is_none() {
                return Err(Error::InvalidRangeResponse);
            }
            self.response = Some(resp);
        }
        let resp = self.response.as_mut().unwrap();

        let mut ret_buf = Vec::with_capacity(size);
        loop {
            let chunk = match self.buffer.take() {
                Some(chunk) => chunk,
                None => match resp.chunk().await {
                    Ok(Some(chunk)) => chunk,
                    // Stream ended. Reset to idle state and return fetched data.
                    Ok(None) => {
                        self.response = None;
                        break;
                    }
                    // Error occurs. Reset to idle state and drop data. (keep data?)
                    Err(err) => {
                        self.response = None;
                        return Err(err.into());
                    }
                },
            };

            let buf_rest_len = size - ret_buf.len();
            if buf_rest_len < chunk.len() {
                self.buffer = Some(chunk.slice(buf_rest_len..));
                ret_buf.extend_from_slice(&chunk[..buf_rest_len]);
                break;
            }
            ret_buf.extend_from_slice(&chunk);
            if ret_buf.len() == size {
                break;
            }
        }

        // Advance current_pos only if everything succeeds.
        self.current_pos += ret_buf.len() as u64;
        Ok(ret_buf.into())
    }
}
