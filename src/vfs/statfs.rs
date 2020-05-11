use crate::error::Result;
use onedrive_api::OneDrive;
use serde::Deserialize;

#[derive(Default)]
pub struct Statfs {}

#[derive(Clone)]
pub struct StatfsData {
    pub total: u64,
    pub free: u64,
}

impl Statfs {
    pub async fn statfs(&self, onedrive: &OneDrive) -> Result<StatfsData> {
        // TODO: Cache
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
