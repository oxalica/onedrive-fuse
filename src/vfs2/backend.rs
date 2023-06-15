use std::pin::Pin;

use anyhow::{ensure, Context, Result};
use futures::future::{BoxFuture, Either};
use futures::stream::BoxStream;
use futures::{AsyncRead, StreamExt, TryFutureExt, TryStreamExt};
use onedrive_api::option::CollectionOption;
pub use onedrive_api::resource::DriveItem;
use onedrive_api::resource::DriveItemField;
use onedrive_api::ItemId;
use reqwest::{header, Client, StatusCode};

use crate::login::ManagedOnedrive;

const PAGE_SIZE: usize = 1024;

#[derive(Debug, thiserror::Error)]
#[error("full sync required")]
pub struct FullSyncRequired;

#[derive(Clone)]
pub struct OnedriveBackend {
    pub onedrive: ManagedOnedrive,
    pub download_client: Client,
}

pub trait Backend {
    fn fetch_changes(&self, delta_url: Option<String>) -> BoxFuture<'static, Result<ChangeStream>>;
    fn download(&self, id: String, offset: u64) -> Pin<Box<dyn AsyncRead + Send + 'static>>;
}

type ChangeStream = BoxStream<'static, Result<Either<DriveItem, String>>>;

impl Backend for OnedriveBackend {
    fn fetch_changes(
        &self,
        delta_url: Option<String>,
    ) -> BoxFuture<'static, Result<BoxStream<'static, Result<Either<DriveItem, String>>>>> {
        let onedrive = self.onedrive.clone();
        Box::pin(async move {
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
            let fetcher = match delta_url {
                None => {
                    onedrive
                        .get()
                        .await
                        .track_root_changes_from_initial_with_option(opts)
                        .await?
                }
                Some(delta_url) => onedrive
                    .get()
                    .await
                    .track_root_changes_from_delta_url(&delta_url)
                    .await
                    .map_err(|err| {
                        if err.status_code() == Some(StatusCode::GONE) {
                            anyhow::Error::from(FullSyncRequired)
                        } else {
                            err.into()
                        }
                    })?,
            };

            let stream = futures::stream::try_unfold(
                (onedrive, fetcher),
                move |(onedrive, mut fetcher)| async move {
                    let page = fetcher.fetch_next_page(&*onedrive.get().await).await?;
                    let ret = if let Some(items) = page {
                        Either::Left(
                            futures::stream::iter(items).map(|item| Ok(Either::Left(item))),
                        )
                    } else {
                        let url = fetcher.delta_url().context("missing delta URL")?.to_owned();
                        Either::Right(futures::stream::iter(Some(Ok(Either::Right(url)))))
                    };
                    anyhow::Ok(Some((ret, (onedrive, fetcher))))
                },
            )
            .try_flatten();
            Ok(Box::pin(stream) as BoxStream<'_, _>)
        })
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
