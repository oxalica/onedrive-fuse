use std::pin::Pin;
use std::time::SystemTime;

use anyhow::{bail, ensure, Context, Result};
use futures::future::{BoxFuture, Either};
use futures::stream::BoxStream;
use futures::{AsyncRead, TryFutureExt, TryStreamExt};
use onedrive_api::option::CollectionOption;
pub use onedrive_api::resource::DriveItem;
use onedrive_api::resource::DriveItemField;
use onedrive_api::{ConflictBehavior, ItemId};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use reqwest::{header, Client, StatusCode};
use serde::{Deserialize, Serialize};

use crate::login::ManagedOnedrive;

const MAX_FETCH_LEN: usize = 1024;

// https://learn.microsoft.com/en-us/graph/json-batching?view=graph-rest-1.0#batch-size-limitations
const MAX_BATCH_LEN: usize = 20;

const API_BATCH_URL: &str = "https://graph.microsoft.com/v1.0/$batch";

#[derive(Debug, thiserror::Error)]
#[error("full sync required")]
pub struct FullSyncRequired;

#[derive(Debug, thiserror::Error)]
#[error("subrequest failed with {0}")]
pub struct SubrequestFailed(pub StatusCode);

pub trait Backend: Clone + Send + 'static {
    fn max_push_len(&self) -> usize;

    fn pull_changes(
        &self,
        delta_url: Option<String>,
    ) -> BoxFuture<'static, Result<RemoteChangeStream>>;

    fn push_changes(
        &self,
        changes: Vec<LocalItemChange>,
    ) -> BoxFuture<'static, Result<Vec<Result<RemoteItemChange>>>>;

    fn download(&self, id: String, offset: u64) -> Pin<Box<dyn AsyncRead + Send + 'static>>;
}

type RemoteChangeStream = BoxStream<'static, Result<Either<RemoteItemChange, String>>>;

#[derive(Debug)]
pub enum RemoteItemChange {
    Update {
        id: String,
        parent_id: Option<String>,
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

#[derive(Debug)]
pub enum LocalItemChange {
    CreateDirectory {
        parent_id: String,
        child_path: String,
        created_time: SystemTime,
        modified_time: SystemTime,
    },
    UpdateTime {
        id: String,
        created_time: SystemTime,
        modified_time: SystemTime,
    },
}

#[derive(Clone)]
pub struct OnedriveBackend {
    pub onedrive: ManagedOnedrive,
    pub download_client: Client,
}

impl Backend for OnedriveBackend {
    fn max_push_len(&self) -> usize {
        MAX_BATCH_LEN
    }

    fn pull_changes(
        &self,
        delta_url: Option<String>,
    ) -> BoxFuture<'static, Result<RemoteChangeStream>> {
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
                .page_size(MAX_FETCH_LEN);
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
                        let items = items.into_iter().filter_map(|item| {
                            match RemoteItemChange::try_from(item) {
                                Ok(change) => Some(Ok(Either::Left(change))),
                                Err(err) => {
                                    log::warn!("Ignored invalid item: {err}");
                                    None
                                }
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

    fn push_changes(
        &self,
        changes: Vec<LocalItemChange>,
    ) -> BoxFuture<'static, Result<Vec<Result<RemoteItemChange>>>> {
        let onedrive = self.onedrive.clone();
        Box::pin(async move {
            log::debug!("Pushing changes: {changes:#?}");

            let request_cnt = changes.len();
            let requests = changes
                .iter()
                .enumerate()
                .map(|(id, change)| BatchSubRequest {
                    id,
                    depends_on: id.checked_sub(1).map(|i| (i,)),
                    inner: change.to_request_inner(),
                })
                .collect::<Vec<_>>();

            let onedrive = onedrive.get().await;
            let resp = onedrive
                .client()
                .post(API_BATCH_URL)
                .bearer_auth(onedrive.access_token())
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::ACCEPT, "application/json")
                .json(&BatchRequest { requests })
                .send()
                .await?;

            let resp: BatchResponse = match resp.error_for_status_ref() {
                Ok(_) => resp.json().await?,
                Err(err) => {
                    let mut err = anyhow::Error::from(err);
                    if let Ok(err_resp) = resp.json::<serde_json::Value>().await {
                        err = err.context(format!("API request failed with {err_resp}"));
                    }
                    return Err(err);
                }
            };

            let mut subresps = resp.responses;
            subresps.sort_by_key(|subresp| subresp.id);
            ensure!(
                subresps.iter().map(|resp| resp.id).eq(0..request_cnt),
                "invalid batch responses: sent 0..{} but got {:?}",
                request_cnt,
                subresps.iter().map(|resp| resp.id).collect::<Vec<_>>(),
            );

            Ok(subresps
                .into_iter()
                .map(|subresp| {
                    StatusCode::from_u16(subresp.status)
                        .map_err(Into::into)
                        .and_then(|status| {
                            ensure!(status.is_success(), "subrequest returns status {status}");
                            Ok(())
                        })
                        .with_context(|| format!("API request failed with {}", subresp.body))?;
                    let item = serde_json::from_value::<RawItemResponse>(subresp.body)?;
                    RemoteItemChange::try_from(item)
                })
                .collect())
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

#[derive(Debug, Deserialize, thiserror::Error)]
#[error("API failure {code}: {message} (innerError={inner_error})")]
#[serde(rename_all = "camelCase")]
struct ErrorResponse {
    code: String,
    message: String,
    #[serde(default)]
    inner_error: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct BatchRequest {
    requests: Vec<BatchSubRequest>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchSubRequest {
    id: usize,
    depends_on: Option<(usize,)>,
    #[serde(flatten)]
    inner: BatchSubRequestInner,
}

#[derive(Debug, Serialize)]
struct BatchSubRequestInner {
    method: &'static str,
    url: String,
    headers: Headers,
    body: RawItemRequest,
}

#[derive(Debug, Serialize)]
struct Headers {
    #[serde(rename = "Content-Type")]
    content_type: ContentType,
}

#[derive(Debug, Serialize)]
enum ContentType {
    #[serde(rename = "application/json")]
    ApplicationJson,
}

#[derive(Debug, Deserialize)]
struct BatchResponse {
    responses: Vec<BatchSubResponse>,
}

#[derive(Debug, Deserialize)]
struct BatchSubResponse {
    // NB. `id` here is always a string though a number is given in the request.
    #[serde(deserialize_with = "serde_aux::prelude::deserialize_number_from_string")]
    id: usize,
    status: u16,
    #[serde(default)]
    body: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RawItemRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    file_system_info: FileSystemInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    folder: Option<EmptySet>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "@microsoft.graph.conflictBehavior"
    )]
    conflict_behavior: Option<ConflictBehavior>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawItemResponse {
    id: String,
    parent_reference: Option<ParentReference>,
    name: String,
    folder: Option<EmptySet>,
    root: Option<EmptySet>,
    size: u64,
    file_system_info: FileSystemInfo,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ParentReference {
    id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmptySet {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileSystemInfo {
    #[serde(rename = "createdDateTime", with = "humantime_serde")]
    created_time: SystemTime,
    #[serde(rename = "lastModifiedDateTime", with = "humantime_serde")]
    modified_time: SystemTime,
}

// TODO: Merge this with `DriveItem` parser.
impl TryFrom<RawItemResponse> for RemoteItemChange {
    type Error = anyhow::Error;

    fn try_from(item: RawItemResponse) -> Result<Self, Self::Error> {
        Ok(Self::Update {
            id: item.id,
            // Root actually has a special parent item. Here we ignore it to prevent confusion.
            parent_id: match item.root {
                Some(_) => None,
                None => Some(item.parent_reference.context("missing parent")?.id),
            },
            // Root also has a name "root". But we do not actually use it.
            name: item.name,
            is_directory: item.folder.is_some(),
            size: if item.folder.is_some() { 0 } else { item.size },
            created_time: item.file_system_info.created_time,
            modified_time: item.file_system_info.modified_time,
        })
    }
}

impl TryFrom<DriveItem> for RemoteItemChange {
    type Error = anyhow::Error;

    fn try_from(item: DriveItem) -> Result<Self, Self::Error> {
        let id = item.id.context("missing id")?.0;

        if item.deleted.is_some() {
            ensure!(item.root.is_none(), "root cannot be deleted");
            return Ok(Self::Delete { id });
        }

        // Root also has a name "root". But we do not actually use it.
        let name = item.name.context("missing name")?;
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
        // Root actually has a special parent item. Here we ignore it to prevent confusion.

        let parent_id = match item.root {
            Some(_) => None,
            None => Some(
                item.parent_reference
                    .as_ref()
                    .and_then(|parent| Some(parent.get("id")?.as_str()?.to_owned()))
                    .context("missing parent id")?,
            ),
        };
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

impl LocalItemChange {
    fn to_request_inner(&self) -> BatchSubRequestInner {
        match *self {
            LocalItemChange::CreateDirectory {
                ref parent_id,
                ref child_path,
                created_time,
                modified_time,
            } => {
                let (intermediate, name) = child_path.rsplit_once('/').expect("invalid child path");
                let intermediate = percent_encode(intermediate.as_bytes(), NON_ALPHANUMERIC);
                BatchSubRequestInner {
                    method: "POST",
                    url: format!("/drives/me/items/{parent_id}:{intermediate}:/children"),
                    headers: Headers {
                        content_type: ContentType::ApplicationJson,
                    },
                    body: RawItemRequest {
                        folder: Some(EmptySet {}),
                        conflict_behavior: Some(ConflictBehavior::Fail),
                        name: Some(name.to_owned()),
                        file_system_info: FileSystemInfo {
                            created_time,
                            modified_time,
                        },
                        id: None,
                    },
                }
            }
            LocalItemChange::UpdateTime {
                ref id,
                created_time,
                modified_time,
            } => BatchSubRequestInner {
                method: "PATCH",
                url: format!("/drives/me/items/{id}"),
                headers: Headers {
                    content_type: ContentType::ApplicationJson,
                },
                body: RawItemRequest {
                    id: Some(id.clone()),
                    file_system_info: FileSystemInfo {
                        created_time,
                        modified_time,
                    },
                    name: None,
                    folder: None,
                    conflict_behavior: None,
                },
            },
        }
    }
}
