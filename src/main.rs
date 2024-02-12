use crate::login::ManagedOnedrive;
use anyhow::{anyhow, Context as _, Result};
use clap::{Args, Parser};
use fuser::MountOption;
use onedrive_api::{Auth, Permission, TokenResponse};
use std::path::PathBuf;
use url::Url;

mod config;
mod fuse_fs;
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

    let opt: Opt = Opt::parse();
    match opt {
        Opt::Login(opt) => main_login(opt).await,
        Opt::Mount(opt) => main_mount(opt).await,
    }
}

const REDIRECT_URI: &str = "http://localhost:0/onedrive-fuse-login";
const HTTP_SERVER_PATH: &str = "/onedrive-fuse-login";

async fn main_login(opt: OptLogin) -> Result<()> {
    let credential_path = opt
        .credential
        .or_else(paths::default_credential_path)
        .context("No credential file provided to save to")?;

    let perm = if opt.read_write {
        "READONLY"
    } else {
        "READWRITE"
    };
    eprintln!("You are logining for {perm} permission.");

    let perm = Permission::new_read()
        .write(opt.read_write)
        .offline_access(true);

    let tokens = if let Some(code) = &opt.code {
        eprintln!("Logining...");
        let auth = Auth::new(opt.client_id.clone(), perm, REDIRECT_URI.to_owned());
        auth.login_with_code(code, None).await?
    } else {
        let client_id = opt.client_id.clone();
        tokio::task::spawn_blocking(|| login_with_http_server(client_id, perm)).await??
    };

    let refresh_token = tokens.refresh_token.expect("Missing refresh token");

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

fn login_with_http_server(client_id: String, perm: Permission) -> Result<TokenResponse> {
    use reqwest::StatusCode;
    use std::io::Cursor;
    use tiny_http::{Header, Response, Server};

    let server = Server::http("localhost:0")
        .map_err(|err| anyhow!("Failed to listen on localhost: {err}"))?;
    let listen_addr = server.server_addr().to_ip().expect("Listen on IP address");
    eprintln!("Listening at {listen_addr} for callback");

    // NB. The host must be exactly `localhost`, which is special to Microsoft API.
    let redirect_uri = format!(
        "http://localhost:{}{}",
        listen_addr.port(),
        HTTP_SERVER_PATH
    );
    let auth = Auth::new(client_id, perm, redirect_uri);
    let auth_url = auth.code_auth_url();

    let _ = open::that(&auth_url);
    eprintln!(
        "\
Please login to your OneDrive (Microsoft) Account in browser.
Your browser should be opened with the login page. If not, please manually open the link below:

{auth_url}

"
    );

    let base_url = Url::parse("http://localhost/").unwrap();
    loop {
        let req = server.recv()?;

        // Unrecognized path. May be unrelated requests from the browser.
        if !req.url().starts_with(HTTP_SERVER_PATH) {
            let _ = req.respond(Response::empty(StatusCode::NOT_FOUND.as_u16()));
            continue;
        }

        let ret = (|| -> Result<_> {
            let url = base_url.join(req.url()).context("Invalid URL")?;
            let code = url
                .query_pairs()
                .find_map(|(key, value)| (key == "code" && !value.is_empty()).then_some(value))
                .context("Missing code")?;
            eprintln!("Logining...");
            let tokens =
                tokio::runtime::Handle::current().block_on(auth.login_with_code(&code, None))?;
            Ok(tokens)
        })();
        let headers =
            vec![Header::from_bytes("content-type", "text/plain; charset=utf-8").unwrap()];
        let (status, ret_str) = match &ret {
            Ok(_) => (
                StatusCode::OK,
                "Login successfully. You can close this page now.".to_owned(),
            ),
            Err(err) => {
                println!("{err:#}");
                (
                    StatusCode::BAD_REQUEST,
                    format!("Login failed. Please close this page and try again.\n{err:#}"),
                )
            }
        };
        let _ = req.respond(Response::new(
            status.as_u16().into(),
            headers,
            Cursor::new(ret_str),
            None,
            None,
        ));

        if let Ok(ret) = ret {
            return Ok(ret);
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
        fuser::FUSE_ROOT_ID,
        readonly,
        config.vfs,
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
    let fs = fuse_fs::Filesystem::new(vfs, config.permission);
    tokio::task::spawn_blocking(move || fuser::mount2(fs, &opt.mount_point, &fuse_options))
        .await??;
    Ok(())
}

#[derive(Debug, Parser)]
#[command(about = "Mount OneDrive storage as FUSE filesystem.")]
#[command(after_help = concat!("\
Copyright (C) 2019-2023, Oxalica
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
#[command(after_help = "\
EXAMPLES:
    # Login with some client id.
    onedrive-fuse --client-id 00000000-0000-0000-0000-000000000000

    # And save credential to a custom path.
    onedrive-fuse -c /path/to/credential --client-id 00000000-0000-0000-0000-000000000000
")]
struct OptLogin {
    /// Secret credential file to save your logined OneDrive account.
    /// Default to be `$HOME/.onedrive/credential.json`.
    #[arg(short, long)]
    credential: Option<PathBuf>,

    /// The client id used for OAuth2.
    #[arg(long)]
    client_id: String,

    /// Request for read-write instead of read-only permission.
    #[arg(short = 'w', long)]
    read_write: bool,

    /// Whether to disable listening on `localhost` for login callback. This will require manually
    /// type the redirected URI to the terminal after login in browser.
    /// Only use this when you are unable to interact with browser on the same compuler (host).
    #[arg(long)]
    no_listen: bool,

    /// The login code for Code-Auth.
    /// If not provided, the program will do interactive login.
    code: Option<String>,
}

#[derive(Debug, Args)]
#[command(after_help = "\
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
    #[arg(short, long)]
    credential: Option<PathBuf>,

    /// Config file to override default settings.
    /// Setting from `--option` has highest priority, followed by `--config`, then the default setting.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Mount point.
    mount_point: PathBuf,

    /// Options to override default settings.
    /// Setting from `--option` has highest priority, followed by `--config`, then the default setting.
    #[arg(short, long)]
    option: Vec<String>,
}
