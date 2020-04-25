use anyhow::{Context as _, Result};
use onedrive_api::{Auth, DriveLocation, OneDrive, Permission};
use serde::Deserialize;
use std::path::PathBuf;

mod fs;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = match parse_args() {
        Ok(args) => args,
        Err(err) => {
            eprintln!("{}", err);
            print_help();
            std::process::exit(1);
        }
    };

    let config: Config = {
        let f = std::fs::File::open(&args.config_file).context("Cannot open config file")?;
        serde_json::from_reader(f).context("Invalid config file")?
    };

    let auth = Auth::new(
        config.client_id.clone(),
        Permission::new_read().offline_access(true),
        config.redirect_uri,
    );
    let tokens = auth
        .login_with_refresh_token(&config.refresh_token, None)
        .await?;
    let onedrive = OneDrive::new(tokens.access_token, DriveLocation::me());

    let fs = fs::OneDriveFilesystem::new(onedrive);
    fuse::mount(fs, &args.mount_point, &[])?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    config_file: PathBuf,
    mount_point: PathBuf,
}

fn parse_args() -> Result<Args> {
    let mut args = pico_args::Arguments::from_env();
    let ret = Args {
        config_file: args.free_from_str()?.context("Missing config path")?,
        mount_point: args.free_from_str()?.context("Missing mount point")?,
    };
    args.finish()?;
    Ok(ret)
}

fn print_help() {
    eprintln!(
        "USAGE: {} <config_file> <mount_point>",
        std::env::args().next().unwrap(),
    );
}

#[derive(Deserialize)]
struct Config {
    client_id: String,
    redirect_uri: String,
    refresh_token: String,
}
