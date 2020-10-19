use anyhow::{Context as _, Result};
use nix::unistd::{getgid, getuid};
use onedrive_api::{Auth, DriveLocation, OneDrive, Permission};
use serde::{Deserialize, Serialize};
use std::{
    env, fs, io,
    os::unix::fs::PermissionsExt as _,
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
        Opt::Login(opt) => main_login(opt).await,
        Opt::Mount(opt) => main_mount(opt).await,
    }
}

const REDIRECT_URI: &str = "https://login.microsoftonline.com/common/oauth2/nativeclient";

async fn main_login(opt: OptLogin) -> Result<()> {
    let credential_path = opt
        .credential
        .or_else(default_credential_path)
        .context("No credential file provided to save to")?;

    let auth = Auth::new(
        opt.client_id.clone(),
        Permission::new_read().offline_access(true),
        REDIRECT_URI.to_owned(),
    );

    let code = match opt.code {
        Some(code) => code,
        None => ask_for_code(&auth.code_auth_url())?,
    };

    eprintln!("Logining...");
    let token_resp = auth.login_with_code(&code, None).await?;
    let refresh_token = token_resp.refresh_token.expect("Missing refresh token");

    eprintln!("Login successfully, saving credential...");

    if let Some(parent) = credential_path.parent() {
        fs::create_dir_all(parent)?;
    }

    {
        let mut f =
            fs::File::create(&credential_path).context("Cannot create or open credential file")?;
        f.set_permissions(fs::Permissions::from_mode(0o644))
            .context("Cannot set permission of credential file")?;
        let credential = Credential {
            client_id: opt.client_id,
            redirect_uri: REDIRECT_URI.to_owned(),
            refresh_token,
        };
        serde_json::to_writer(&mut f, &credential)?;
    }

    Ok(())
}

fn ask_for_code(auth_url: &str) -> Result<String> {
    let _ = open::that(auth_url);
    eprintln!(
        "\
Your browser should be opened. If not, please manually open the link below:
{}

Login to your OneDrive (Microsoft) Account in the link, it will jump to a blank page
whose URL contains `nativeclient?code=`.
",
        auth_url
    );

    loop {
        eprintln!(
            "Please copy and paste the FULL URL of the blank page here and then press ENTER:"
        );
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        let line = line.trim();

        const NEEDLE: &str = "nativeclient?code=";
        match line.find(NEEDLE) {
            Some(pos) => return Ok(line[pos + NEEDLE.len()..].to_owned()),
            _ => eprintln!("Invalid URL."),
        }
    }
}

async fn main_mount(opt: OptMount) -> Result<()> {
    let credential_path = opt
        .credential
        .or_else(default_credential_path)
        .context("No credential file provided")?;
    let mut credential: Credential = {
        let f = fs::File::open(&credential_path).context("Cannot open credential file")?;
        serde_json::from_reader(f).context("Invalid credential file")?
    };

    let config = load_vfs_config(opt.config.as_deref())?;

    log::info!("Logining...");
    let auth = Auth::new(
        credential.client_id.clone(),
        Permission::new_read().offline_access(true),
        credential.redirect_uri.clone(),
    );
    let token_resp = auth
        .login_with_refresh_token(&credential.refresh_token, None)
        .await?;
    credential.refresh_token = token_resp.refresh_token.unwrap();

    log::info!("Updating credential...");
    if let Err(err) = (|| -> Result<_> {
        let mut f =
            fs::File::create(&credential_path).context("Cannot create or open credential file")?;
        f.set_permissions(fs::Permissions::from_mode(0o644))
            .context("Cannot set permission of credential file")?;
        serde_json::to_writer(&mut f, &credential)?;
        Ok(())
    })() {
        log::warn!(
            "Cannot update credential file. It may be expired without refreshing. {}",
            err
        );
    }

    let onedrive = OneDrive::new(token_resp.access_token, DriveLocation::me());

    log::info!("Mounting...");
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

#[derive(Debug, Serialize, Deserialize)]
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
    /// Login to your OneDrive (Microsoft) account.
    Login(OptLogin),
    /// Mount OneDrive storage.
    Mount(OptMount),
}

#[derive(Debug, StructOpt)]
struct OptLogin {
    /// Secret credential file to save your logined OneDrive account.
    /// Default to be `$HOME/.onedrive/credential.json`.
    #[structopt(short, long, parse(from_os_str))]
    credential: Option<PathBuf>,

    /// The client id used for OAuth2.
    #[structopt(long)]
    client_id: String,

    /// The login code for Code-Auth.
    /// If not provided, the program will interactively open your browser and
    /// ask for the redirected URL containing it.
    code: Option<String>,
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
