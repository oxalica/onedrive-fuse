use reqwest::StatusCode;
use std::ffi::OsString;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // User errors.
    #[error("Object not found")]
    NotFound,
    #[error("Invalid file name: {}", .0.to_string_lossy())]
    InvalidFileName(OsString),

    // Api and network errors.
    #[error("Api error: {0}")]
    ApiError(onedrive_api::Error),
    #[error("Api error (deserialization): {0}")]
    ApiDeserializeError(#[from] serde_json::Error),
    #[error("reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("Unexpected end of download stream, read {current_pos}/{file_size}")]
    UnexpectedEndOfDownload { current_pos: u64, file_size: u64 },

    // IO error.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    // Not supported.
    #[error("Nonsequential read is not supported: current at {current_pos} but try to read {try_offset}")]
    NonsequentialRead { current_pos: u64, try_offset: u64 },

    // Fuse errors.
    // They are hard errors here, since `fuse` should guarantee that they are valid.
    #[error("Invalid inode: {0}")]
    InvalidInode(u64),
    #[error("Invalid handle: {0}")]
    InvalidHandle(u64),
}

impl From<onedrive_api::Error> for Error {
    fn from(err: onedrive_api::Error) -> Self {
        match err.status_code() {
            Some(StatusCode::NOT_FOUND) => Self::NotFound,
            // TODO: Handle CONFLICT?
            _ => Self::ApiError(err),
        }
    }
}

impl Error {
    pub fn into_c_err(self) -> libc::c_int {
        match &self {
            // User errors.
            Self::NotFound => libc::ENOENT,
            Self::InvalidFileName(_) => {
                log::info!("{}", self);
                libc::EINVAL
            }

            // Network errors.
            Self::ApiError(_)
            | Self::ApiDeserializeError(_)
            | Self::ReqwestError(_)
            | Self::UnexpectedEndOfDownload { .. }
            | Self::IoError(_) => {
                log::error!("{}", self);
                libc::EIO
            }

            // Not supported
            Self::NonsequentialRead { .. } => {
                log::info!("{}", self);
                libc::EPERM
            }

            // Fuse errors.
            Self::InvalidInode(_) | Self::InvalidHandle(_) => {
                panic!("Invalid arguments from `fuse`: {}", self);
            }
        }
    }
}
