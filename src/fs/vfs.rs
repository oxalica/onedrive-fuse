use super::CResult;
use anyhow::Result as AResult;
use onedrive_api::OneDrive;

trait ResultExt<T> {
    fn io_err(self, target: &'static str) -> CResult<T>;
}

impl<T, E: std::fmt::Display> ResultExt<T> for Result<T, E> {
    fn io_err(self, target: &'static str) -> CResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => {
                log::error!(target: target, "{}", err);
                Err(libc::EIO)
            }
        }
    }
}

#[derive(Clone)]
pub struct Statfs {
    pub total: u64,
    pub free: u64,
}

#[derive(Default)]
pub struct Vfs {}

impl Vfs {
    pub async fn statfs(&self, onedrive: &OneDrive) -> CResult<Statfs> {
        // TODO: Cache
        self.statfs_raw(onedrive).await.io_err("statfs")
    }

    async fn statfs_raw(&self, onedrive: &OneDrive) -> AResult<Statfs> {
        use onedrive_api::{option::ObjectOption, resource::DriveField};

        #[derive(Debug, serde::Deserialize)]
        struct Quota {
            total: u64,
            remaining: u64,
            // used: u64,
        }

        let drive = onedrive
            .get_drive_with_option(ObjectOption::new().select(&[DriveField::quota]))
            .await?;
        let quota: Quota = serde_json::from_value(*drive.quota.unwrap())?;
        Ok(Statfs {
            total: quota.total,
            free: quota.remaining,
        })
    }
}
