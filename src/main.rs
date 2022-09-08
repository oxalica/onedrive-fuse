use crate::login::ManagedOnedrive;
use anyhow::{Context as _, Result};
use clap::{Args, Parser};
use fuser::MountOption;
use onedrive_api::{Auth, Permission};
use std::io;
use std::path::PathBuf;

mod api;
mod config;
mod error;
mod inode;
mod login;
mod paths;
mod vfs;

#[tokio::main]
async fn main() -> Result<()> {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        // Immediately exit the whole program when any (async) thread panicked.
        std::process::exit(101);
    }));

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
        .or_else(paths::default_credential_path)
        .context("No credential file provided to save to")?;

    let auth = Auth::new(
        opt.client_id.clone(),
        Permission::new_read()
            .write(opt.read_write)
            .offline_access(true),
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
        readonly: !opt.read_write,
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
        .or_else(paths::default_credential_path)
        .context("No credential file provided")?;

    let config = config::Config::merge_from_default(opt.config.as_deref(), &opt.option)?;
    let readonly = config.permission.readonly;

    let client = reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .gzip(true)
        .https_only(true)
        .connect_timeout(config.net.connect_timeout)
        .timeout(config.net.request_timeout)
        .build()?;
    let unlimit_client = reqwest::ClientBuilder::new()
        .https_only(true)
        .connect_timeout(config.net.connect_timeout)
        .build()?;

    let onedrive =
        ManagedOnedrive::login(client, credential_path, config.relogin, readonly).await?;
    let vfs = vfs::Vfs::new(
        config.vfs,
        config.permission.clone(),
        onedrive.clone(),
        unlimit_client,
    )
    .await
    .context("Failed to initialize vfs")?;

    log::info!("Mounting...");
    let fuse_options = [
        MountOption::FSName("onedrive".into()),
        MountOption::DefaultPermissions, // Check permission in the kernel.
        MountOption::NoDev,
        MountOption::NoSuid,
        MountOption::NoAtime,
        if config.permission.executable {
            MountOption::Exec
        } else {
            MountOption::NoExec
        },
        if readonly {
            MountOption::RO
        } else {
            MountOption::RW
        },
    ];
    tokio::task::spawn_blocking(move || fuser::mount2(vfs, &opt.mount_point, &fuse_options))
        .await??;
    Ok(())
}

#[derive(Debug, Parser)]
#[clap(about = "Mount OneDrive storage as FUSE filesystem.")]
#[clap(after_help = concat!("\
Copyright (C) 2019-2022, Oxalica
This is free software; see the source for copying conditions. There is NO warranty;
not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
"))]
enum Opt {
    /// Login to your OneDrive (Microsoft) account.
    Login(OptLogin),
    /// Mount OneDrive storage.
    Mount(OptMount),
}

#[derive(Debug, Args)]
#[clap(after_help = "\
EXAMPLES:
    # Login with some client id.
    onedrive-fuse --client-id 00000000-0000-0000-0000-000000000000

    # And save credential to a custom path.
    onedrive-fuse -c /path/to/credential --client-id 00000000-0000-0000-0000-000000000000
")]
struct OptLogin {
    /// Secret credential file to save your logined OneDrive account.
    /// Default to be `$HOME/.onedrive/credential.json`.
    #[clap(short, long, parse(from_os_str))]
    credential: Option<PathBuf>,

    /// The client id used for OAuth2.
    #[clap(long)]
    client_id: String,

    /// Request for read-write instead of read-only permission.
    #[clap(short = 'w', long)]
    read_write: bool,

    /// The login code for Code-Auth.
    /// If not provided, the program will interactively open your browser and
    /// ask for the redirected URL containing it.
    code: Option<String>,
}

#[derive(Debug, Args)]
#[clap(after_help = "\
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
    #[clap(short, long, parse(from_os_str))]
    credential: Option<PathBuf>,

    /// Config file to override default settings.
    /// Setting from `--option` has highest priority, followed by `--config`, then the default setting.
    #[clap(long, parse(from_os_str))]
    config: Option<PathBuf>,

    /// Mount point.
    #[clap(parse(from_os_str))]
    mount_point: PathBuf,

    /// Options to override default settings.
    /// Setting from `--option` has highest priority, followed by `--config`, then the default setting.
    #[clap(short, long)]
    option: Vec<String>,
}
