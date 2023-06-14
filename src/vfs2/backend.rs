use std::pin::Pin;

use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use futures::{AsyncRead, TryFutureExt, TryStreamExt};
use onedrive_api::option::CollectionOption;
pub use onedrive_api::resource::DriveItem;
use onedrive_api::resource::DriveItemField;
use onedrive_api::ItemId;
use reqwest::{header, Client, StatusCode};

use crate::login::ManagedOnedrive;

const PAGE_SIZE: usize = 1024;

#[derive(Clone)]
pub struct OnedriveBackend {
    pub onedrive: ManagedOnedrive,
    pub download_client: Client,
}

#[async_trait]
pub trait Backend {
    async fn sync(&self, delta_url: Option<&str>) -> Result<(Vec<DriveItem>, String)>;
    fn download(&self, id: String, offset: u64) -> Pin<Box<dyn AsyncRead + Send + 'static>>;
}

#[async_trait]
impl Backend for OnedriveBackend {
    async fn sync(&self, delta_url: Option<&str>) -> Result<(Vec<DriveItem>, String)> {
        if delta_url.is_some() {
            log::info!("Synchronizing (incremental)");
        } else {
            log::info!("Synchronizing (full)");
        }

        let opts = CollectionOption::new()
            .select(&[
                DriveItemField::id,
                DriveItemField::name,
                DriveItemField::size,
                DriveItemField::parent_reference,
                DriveItemField::file_system_info,
                DriveItemField::root,
                DriveItemField::file,
                DriveItemField::folder,
                DriveItemField::deleted,
            ])
            .page_size(PAGE_SIZE);
        let drive = self.onedrive.get().await;
        let mut fetcher = match delta_url {
            None => {
                drive
                    .track_root_changes_from_initial_with_option(opts)
                    .await?
            }
            Some(delta_url) => drive.track_root_changes_from_delta_url(delta_url).await?,
        };

        let mut items = Vec::new();
        while let Some(batch) = fetcher.fetch_next_page(&drive).await? {
            items.extend(batch);
            log::info!("Received {} changes", items.len());
        }

        let delta_url = fetcher.delta_url().context("missing delta URL")?.to_owned();
        Ok((items, delta_url))
    }

    fn download(&self, id: String, offset: u64) -> Pin<Box<dyn AsyncRead + Send + 'static>> {
        let this = self.clone();
        let stream = async move {
            let drive = this.onedrive.get().await;
            let url = drive.get_item_download_url(&ItemId(id)).await?;
            let mut req = this.download_client.get(url);
            let expect_status = if offset != 0 {
                req = req.header(header::RANGE, format!("bytes={offset}-"));
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            };
            let resp = req.send().await?;
            ensure!(
                resp.status() == expect_status,
                "request failed with {}",
                resp.status(),
            );
            Ok(resp.bytes_stream().map_err(Into::into))
        }
        .try_flatten_stream()
        .map_err(|err: anyhow::Error| std::io::Error::new(std::io::ErrorKind::Other, err))
        .into_async_read();
        Box::pin(stream)
    }
}
