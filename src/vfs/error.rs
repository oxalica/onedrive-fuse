use reqwest::StatusCode;
use std::ffi::OsString;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // User errors.
    #[error("Read-only filesystem")]
    Readonly,
    #[error("Object not found")]
    NotFound,
    #[error("Not a directory")]
    NotADirectory,
    #[error("Is a directory")]
    IsADirectory,
    #[error("Directory not empty")]
    DirectoryNotEmpty,
    #[error("Invalid file name: {}", .0.to_string_lossy())]
    InvalidFileName(OsString),
    #[error("File exists")]
    FileExists,
    #[error("File changed in remote side, please re-open it")]
    Invalidated,
    #[error("File is uploading, you cannot move or remove it")]
    Uploading,

    // Api and network errors.
    #[error("Api error: {0}")]
    ApiError(onedrive_api::Error),
    #[error("Api error (deserialization): {0}")]
    ApiDeserializeError(#[from] serde_json::Error),
    #[error("reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("Download failed")]
    DownloadFailed,

    // IO error.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    // Not supported.
    #[error("Nonsequential read is not supported: current at {current_pos} but try to read {read_size} at {read_offset}")]
    NonsequentialRead {
        current_pos: u64,
        read_offset: u64,
        read_size: usize,
    },
    #[error("File is too large to write")]
    FileTooLarge,
    #[error("File writing is not supported without disk cache")]
    WriteWithoutCache,

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
            Some(StatusCode::CONFLICT) => Self::FileExists,
            _ => Self::ApiError(err),
        }
    }
}

impl Error {
    pub fn into_c_err(self) -> libc::c_int {
        match &self {
            // User errors.
            Self::Readonly => libc::EROFS,
            Self::NotFound => libc::ENOENT,
            Self::NotADirectory => libc::ENOTDIR,
            Self::IsADirectory => libc::EISDIR,
            Self::DirectoryNotEmpty => libc::ENOTEMPTY,
            Self::FileExists => libc::EEXIST,
            Self::Invalidated => libc::EPERM,
            Self::Uploading => libc::ETXTBSY,
            Self::InvalidFileName(_) => {
                log::info!("{}", self);
                libc::EINVAL
            }

            // Network errors.
            Self::ApiError(_)
            | Self::ApiDeserializeError(_)
            | Self::ReqwestError(_)
            | Self::IoError(_) => {
                log::error!("{}", self);
                log::debug!("{:?}", self);
                libc::EIO
            }
            // Already reported.
            Self::DownloadFailed => libc::EIO,

            // Not supported
            Self::NonsequentialRead { .. } | Self::FileTooLarge | Self::WriteWithoutCache => {
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
