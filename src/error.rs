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

    // Api errors.
    #[error("Api error: {0}")]
    ApiError(onedrive_api::Error),
    #[error("Api error (deserialization): {0}")]
    ApiDeserializeError(#[from] serde_json::Error),

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

pub trait IntoCError {
    /// Log internal error if needed and return errno.
    fn into_c_err(self) -> libc::c_int;
}

impl IntoCError for Error {
    fn into_c_err(self) -> libc::c_int {
        match &self {
            Self::NotFound => libc::ENOENT,
            Self::InvalidFileName(_) => {
                log::info!("{}", self);
                libc::EINVAL
            }

            Self::ApiError(_) | Self::ApiDeserializeError(_) => {
                log::error!("{}", self);
                libc::EIO
            }

            Self::InvalidInode(_) | Self::InvalidHandle(_) => {
                panic!("Invalid arguments from `fuse`: {}", self);
            }
        }
    }
}
