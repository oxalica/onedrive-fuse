use crate::config::PermissionConfig;
use crate::login::ManagedOnedrive;
use fuser::Filesystem;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct Config {}

pub struct Vfs {}

impl Vfs {
    // TODO
    pub async fn new(
        _config: Config,
        _permission: PermissionConfig,
        _onedrive: ManagedOnedrive,
        _client: reqwest::Client,
    ) -> anyhow::Result<Arc<Self>> {
        let this = Arc::new(Self {});
        Ok(this)
    }
}

impl Filesystem for &'_ Vfs {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        log::info!("FUSE initialized");
        let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]);
        Ok(())
    }

    fn destroy(&mut self) {
        log::info!("FUSE destroyed")
    }
}
