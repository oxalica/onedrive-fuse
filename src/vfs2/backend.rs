use std::pin::Pin;
use std::time::SystemTime;

use anyhow::{bail, ensure, Context, Result};
use futures::future::{BoxFuture, Either};
use futures::stream::BoxStream;
use futures::{AsyncRead, TryFutureExt, TryStreamExt};
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

pub trait Backend: Clone + Send + 'static {
    fn fetch_changes(&self, delta_url: Option<String>) -> BoxFuture<'static, Result<ChangeStream>>;
    fn download(&self, id: String, offset: u64) -> Pin<Box<dyn AsyncRead + Send + 'static>>;
}

type ChangeStream = BoxStream<'static, Result<Either<ItemChange, String>>>;

#[derive(Debug)]
pub enum ItemChange {
    RootUpdate {
        id: String,
        created_time: SystemTime,
        modified_time: SystemTime,
    },
    Update {
        id: String,
        parent_id: String,
        name: String,
        is_directory: bool,
        size: u64,
        created_time: SystemTime,
        modified_time: SystemTime,
    },
    Delete {
        id: String,
    },
}

impl TryFrom<DriveItem> for ItemChange {
    type Error = anyhow::Error;

    fn try_from(item: DriveItem) -> Result<Self, Self::Error> {
        let id = item.id.context("missing id")?.0;

        if item.deleted.is_some() {
            ensure!(item.root.is_none(), "root cannot be deleted");
            return Ok(Self::Delete { id });
        }

        let fsinfo = item.file_system_info.context("missing fileSystemInfo")?;
        let parse_time = |field: &str| {
            let time = fsinfo
                .get(field)
                .and_then(|v| v.as_str())
                .context("missing field")?;
            humantime::parse_rfc3339(time).with_context(|| format!("invalid format: {time:?}"))
        };
        let created_time = parse_time("createdDateTime").context("failed to get creation time")?;
        let modified_time =
            parse_time("lastModifiedDateTime").context("failed to get modified time")?;

        if item.root.is_some() {
            return Ok(Self::RootUpdate {
                id,
                created_time,
                modified_time,
            });
        }

        let name = item
            .name
            .filter(|name| !name.is_empty())
            .context("missing name")?;
        let is_directory = match (item.file.is_some(), item.folder.is_some()) {
            (true, false) => false,
            (false, true) => true,
            _ => bail!("unknown file type"),
        };
        let size = if is_directory {
            0
        } else {
            *item.size.as_ref().context("missing size")? as u64
        };
        let parent_id = item
            .parent_reference
            .as_ref()
            .and_then(|v| v.get("id")?.as_str())
            .context("missing parent id")?
            .to_owned();
        Ok(Self::Update {
            id,
            parent_id,
            name,
            is_directory,
            size,
            created_time,
            modified_time,
        })
    }
}

impl Backend for OnedriveBackend {
    fn fetch_changes(&self, delta_url: Option<String>) -> BoxFuture<'static, Result<ChangeStream>> {
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
                        let items =
                            items
                                .into_iter()
                                .filter_map(|item| match ItemChange::try_from(item) {
                                    Ok(change) => Some(Ok(Either::Left(change))),
                                    Err(err) => {
                                        log::warn!("Ignored invalid item: {err}");
                                        None
                                    }
                                });
                        Either::Left(futures::stream::iter(items))
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
