use crate::{
    config::de_duration_sec,
    login::ManagedOnedrive,
    vfs::error::{Error, Result},
};
use onedrive_api::OneDrive;
use serde::Deserialize;
use std::{
    sync::{Arc, Mutex as SyncMutex, Weak},
    time::Duration,
};

pub struct Statfs {
    cache: Arc<SyncMutex<StatfsData>>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    enable_auto_refresh: bool,
    #[serde(deserialize_with = "de_duration_sec")]
    refresh_period: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct StatfsData {
    pub total: u64,
    pub free: u64,
}

impl Statfs {
    pub async fn new(onedrive: ManagedOnedrive, config: Config) -> Result<Self> {
        let data = Self::statfs_raw(&*onedrive.get().await).await?;
        let cache = Arc::new(SyncMutex::new(data));
        if config.enable_auto_refresh {
            tokio::spawn(Self::refresh_thread(
                Arc::downgrade(&cache),
                config.refresh_period,
                onedrive,
            ));
        }
        Ok(Self { cache })
    }

    async fn refresh_thread(
        this: Weak<SyncMutex<StatfsData>>,
        period: Duration,
        onedrive: ManagedOnedrive,
    ) {
        loop {
            // We don't need to catch up.
            tokio::time::sleep(period).await;

            let this = match this.upgrade() {
                Some(arc) => arc,
                None => return,
            };
            let data = match Self::statfs_raw(&*onedrive.get().await).await {
                Ok(data) => data,
                Err(err) => {
                    log::error!("Failed to query quota: {}", err);
                    continue;
                }
            };
            *this.lock().unwrap() = data;
            log::debug!("Quota refreshed: {:?}", data);
        }
    }

    pub fn statfs(&self) -> StatfsData {
        *self.cache.lock().unwrap()
    }

    async fn statfs_raw(onedrive: &OneDrive) -> Result<StatfsData> {
        use onedrive_api::{option::ObjectOption, resource::DriveField};

        #[derive(Debug, Deserialize)]
        struct Quota {
            total: u64,
            remaining: u64,
            // used: u64,
        }

        let drive = onedrive
            .get_drive_with_option(ObjectOption::new().select(&[DriveField::quota]))
            .await?;
        let quota: Quota =
            serde_json::from_value(*drive.quota.unwrap()).map_err(Error::Deserialize)?;
        Ok(StatfsData {
            total: quota.total,
            free: quota.remaining,
        })
    }
}
