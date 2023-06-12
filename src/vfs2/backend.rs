use anyhow::{Context, Result};
use async_trait::async_trait;
use onedrive_api::option::CollectionOption;
pub use onedrive_api::resource::DriveItem;
use onedrive_api::resource::DriveItemField;

use crate::login::ManagedOnedrive;

const PAGE_SIZE: usize = 1024;

#[async_trait]
pub trait Backend {
    async fn sync(&self, delta_url: Option<&str>) -> Result<(Vec<DriveItem>, String)>;
}

#[async_trait]
impl Backend for ManagedOnedrive {
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
        let drive = self.get().await;
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
}
