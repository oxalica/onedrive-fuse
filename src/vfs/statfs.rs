use crate::{error::Result, util::de_duration_sec};
use onedrive_api::OneDrive;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub struct Statfs {
    config: Config,
    cache: Mutex<Option<(StatfsData, Instant)>>,
}

#[derive(Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "de_duration_sec")]
    cache_ttl: Duration,
}

#[derive(Clone, Copy)]
pub struct StatfsData {
    pub total: u64,
    pub free: u64,
}

impl Statfs {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            cache: Default::default(),
        }
    }

    pub async fn statfs(&self, onedrive: &OneDrive) -> Result<(StatfsData, Duration)> {
        let mut cache = self.cache.lock().await;
        if let Some((last_data, last_inst)) = &*cache {
            if let Some(ttl) = self.config.cache_ttl.checked_sub(last_inst.elapsed()) {
                return Ok((*last_data, ttl));
            }
        }

        // Cache miss.
        log::debug!(target: "cache", "statfs: cache miss");
        let data = self.statfs_raw(onedrive).await?;
        // Fresh cache.
        *cache = Some((data, Instant::now()));
        Ok((data, self.config.cache_ttl))
    }

    async fn statfs_raw(&self, onedrive: &OneDrive) -> Result<StatfsData> {
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
            serde_json::from_value(*drive.quota.unwrap()).expect("Deserialize error");
        Ok(StatfsData {
            total: quota.total,
            free: quota.remaining,
        })
    }
}
