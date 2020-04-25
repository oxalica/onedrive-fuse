use fuse::*;
use libc::c_int;
use log::*;
use onedrive_api::OneDrive;
use std::ffi::OsStr;

pub struct OneDriveFilesystem {
    onedrive: OneDrive,
}

impl OneDriveFilesystem {
    pub fn new(onedrive: OneDrive) -> Self {
        Self { onedrive }
    }
}

impl Filesystem for OneDriveFilesystem {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        info!("Initialized");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        info!("Destroyed");
    }
}
