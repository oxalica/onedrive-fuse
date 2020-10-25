use crate::login::ManagedOnedrive;
use anyhow::{Context as _, Result};
use onedrive_api::{Auth, Permission};
use std::{env, fs, io, path::PathBuf};
use structopt::StructOpt;

mod config;
mod fuse_fs;
mod login;
mod vfs;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

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

    login::Credential {
        client_id: opt.client_id,
        redirect_uri: REDIRECT_URI.to_owned(),
        refresh_token,
    }
    .save(&credential_path)
    .context("Cannot save credential file")?;

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

    let config = config::Config::merge_from_default(opt.config.as_deref(), &opt.option)?;

    let onedrive = ManagedOnedrive::login(credential_path, config.relogin).await?;
    let vfs = vfs::Vfs::new(config.vfs, onedrive.clone())
        .await
        .context("Failed to initialize vfs")?;

    log::info!("Mounting...");
    let fs = fuse_fs::Filesystem::new(vfs, config.permission);
    let mount_point = opt.mount_point;
    tokio::task::spawn_blocking(move || fuse::mount(fs, &mount_point, &[])).await??;
    Ok(())
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
#[structopt(after_help = "\
EXAMPLES:
    # Login with some client id.
    onedrive-fuse --client-id 00000000-0000-0000-0000-000000000000

    # And save credential to a custom path.
    onedrive-fuse -c /path/to/credential --client-id 00000000-0000-0000-0000-000000000000
")]
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
#[structopt(after_help = "\
EXAMPLES:
    # Using default credential file to mount OneDrive on `~/mnt`.
    onedrive-fuse mount ~/mnt

    # Use custom credential file.
    onedrive-fuse mount -c /path/to/credential ~/mnt

    # Modify some default settings.
    onedrive-fuse mount -o permission.umask=0o077 -o relogin.enable=false ~/mnt
")]
struct OptMount {
    /// Secret credential file to login OneDrive account.
    /// Default to be `$HOME/.onedrive_fuse/credential.json`.
    #[structopt(short, long, parse(from_os_str))]
    credential: Option<PathBuf>,

    /// Config file to override default settings.
    /// Setting from `--option` has highest priority, followed by `--config`, then the default setting.
    #[structopt(long, parse(from_os_str))]
    config: Option<PathBuf>,

    /// Mount point.
    #[structopt(parse(from_os_str))]
    mount_point: PathBuf,

    /// Options to override default settings.
    /// Setting from `--option` has highest priority, followed by `--config`, then the default setting.
    #[structopt(short, long)]
    option: Vec<String>,
}

fn default_credential_path() -> Option<PathBuf> {
    let mut path = PathBuf::from(env::var("HOME").ok()?);
    path.push(".onedrive_fuse");
    path.push("credential.json");
    Some(path)
}
