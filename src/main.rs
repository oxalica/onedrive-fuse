use anyhow::{Context as _, Result};
use nix::unistd::{getgid, getuid};
use onedrive_api::{Auth, DriveLocation, OneDrive, Permission};
use serde::Deserialize;
use std::{
    env, fs,
    path::{Path, PathBuf},
};
use structopt::StructOpt;

mod error;
mod fuse_fs;
mod util;
mod vfs;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let opt: Opt = Opt::from_args();
    match opt {
        Opt::Mount(opt) => main_mount(opt).await,
    }
}

async fn main_mount(opt: OptMount) -> Result<()> {
    let credential: Credential = {
        let path = opt
            .credential
            .or_else(default_credential_path)
            .context("No credential file provided")?;
        let f = fs::File::open(&path).context("Cannot open credential file")?;
        serde_json::from_reader(f).context("Invalid credential file")?
    };

    let config = load_vfs_config(opt.config.as_deref())?;

    let auth = Auth::new(
        credential.client_id.clone(),
        Permission::new_read().offline_access(true),
        credential.redirect_uri,
    );
    let tokens = auth
        .login_with_refresh_token(&credential.refresh_token, None)
        .await?;
    // TODO: Save new refresh token.
    let onedrive = OneDrive::new(tokens.access_token, DriveLocation::me());

    let (uid, gid) = (getuid().as_raw(), getgid().as_raw());
    let fs = fuse_fs::Filesystem::new(onedrive, uid, gid, config)
        .await
        .context("Cannot initialize vfs")?;
    let mount_point = opt.mount_point;
    tokio::task::spawn_blocking(move || fuse::mount(fs, &mount_point, &[])).await??;
    Ok(())
}

fn load_vfs_config(config: Option<&Path>) -> Result<vfs::Config> {
    use config::{Config, File, FileFormat};
    const DEFAULT_CONFIG: &str = include_str!("../config.default.toml");

    let mut conf = Config::new();
    conf.merge(File::from_str(DEFAULT_CONFIG, FileFormat::Toml))?;
    if let Some(path) = config {
        let path = path.to_str().context("Invalid config file path")?;
        conf.merge(File::new(path, FileFormat::Toml))?;
    }
    Ok(conf.try_into()?)
}

#[derive(Deserialize)]
struct Credential {
    client_id: String,
    redirect_uri: String,
    refresh_token: String,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "Mount OneDrive storage as FUSE filesystem.")]
#[structopt(after_help = concat!("\
Copyright (C) 2019-2020 ", env!("CARGO_PKG_AUTHORS"), "
This is free software; see the source for copying conditions. There is NO warranty;
not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
"))]
enum Opt {
    /// Mount OneDrive storage.
    Mount(OptMount),
}

#[derive(Debug, StructOpt)]
struct OptMount {
    /// Secret credential file to login OneDrive account.
    /// Default to be `$HOME/.onedrive_fuse/credential.json`.
    #[structopt(short, long, parse(from_os_str))]
    credential: Option<PathBuf>,

    /// Optional config file to adjust internal settings.
    #[structopt(long, parse(from_os_str))]
    config: Option<PathBuf>,

    /// Mount point.
    #[structopt(parse(from_os_str))]
    mount_point: PathBuf,
}

fn default_credential_path() -> Option<PathBuf> {
    let mut path = PathBuf::from(env::var("HOME").ok()?);
    path.push(".onedrive_fuse");
    path.push("credential.json");
    Some(path)
}
