use reqwest::StatusCode;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Api error: {0}")]
    ApiError(onedrive_api::Error),
    #[error("Object not found")]
    NotFound,
    #[error("Invalid argument")]
    InvalidArgument(&'static str),
}

impl From<onedrive_api::Error> for Error {
    fn from(err: onedrive_api::Error) -> Self {
        match err.status_code() {
            Some(StatusCode::NOT_FOUND) => Self::NotFound,
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
        match self {
            Self::ApiError(err) => {
                log::error!("Api error: {}", err);
                libc::EIO
            }
            Self::NotFound => libc::ENOENT,
            Self::InvalidArgument(reason) => {
                log::info!("Invalid argument: {}", reason);
                libc::EINVAL
            }
        }
    }
}
