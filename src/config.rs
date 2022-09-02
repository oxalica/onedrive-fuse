use crate::{login, vfs};
use anyhow::{Context as _, Result};
use libc::{gid_t, mode_t, uid_t};
use serde::{de::Deserializer, Deserialize};
use std::{path::Path, time::Duration};

const DEFAULT_CONFIG: &str = include_str!("../config.default.toml");

#[derive(Debug, Deserialize)]
pub struct Config {
    pub permission: PermissionConfig,
    pub vfs: vfs::Config,
    pub relogin: login::ReloginConfig,
    pub net: NetConfig,
}

#[derive(Debug, Deserialize)]
pub struct NetConfig {
    #[serde(deserialize_with = "de_duration_sec")]
    pub connect_timeout: Duration,
    #[serde(deserialize_with = "de_duration_sec")]
    pub request_timeout: Duration,
}

impl Config {
    pub fn merge_from_default(config_path: Option<&Path>, options: &[String]) -> Result<Self> {
        use config::{File, FileFormat};

        let mut builder = config::Config::builder();
        builder = builder.add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml));
        if let Some(path) = config_path {
            builder = builder.add_source(File::from(path).format(FileFormat::Toml));
        }
        for opt in options {
            // Kind of tricky. Toml can parse option format `a.b="foo"` as expected.
            builder = builder.add_source(File::from_str(opt, FileFormat::Toml));
        }
        builder
            .build()
            .and_then(|conf| conf.try_deserialize())
            .context("Failed to load configuration")
    }
}

#[derive(Debug, Deserialize)]
pub struct PermissionConfig {
    pub readonly: bool,
    pub executable: bool,
    #[serde(default = "get_uid")]
    pub uid: libc::uid_t,
    #[serde(default = "get_gid")]
    pub gid: libc::uid_t,
    #[serde(default = "get_umask")]
    umask: mode_t,
    #[serde(default)]
    fmask: mode_t,
    #[serde(default)]
    dmask: mode_t,
}

impl PermissionConfig {
    fn umask(&self) -> mode_t {
        if self.readonly {
            self.umask | 0o222
        } else {
            self.umask
        }
    }

    pub fn file_permission(&self) -> mode_t {
        0o666 & !(self.umask() | self.fmask)
    }

    pub fn dir_permission(&self) -> mode_t {
        0o777 & !(self.umask() | self.dmask)
    }
}

fn get_uid() -> uid_t {
    nix::unistd::getuid().as_raw()
}

fn get_gid() -> gid_t {
    nix::unistd::getgid().as_raw()
}

fn get_umask() -> mode_t {
    use nix::sys::stat::{umask, Mode};
    let prev = umask(Mode::empty());
    umask(prev);
    prev.bits()
}

pub fn de_duration_sec<'de, D>(de: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    u64::deserialize(de).map(Duration::from_secs)
}
