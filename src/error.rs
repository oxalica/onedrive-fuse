pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub enum Error {
    FileExists,
    InvalidArgument,
    IsNotDirectory,
    NotFound,

    Protocol(String),
}

impl From<&'_ str> for Error {
    fn from(reason: &'_ str) -> Self {
        Self::Protocol(reason.into())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Protocol(e.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for Error {}

impl From<Error> for libc::c_int {
    fn from(e: Error) -> Self {
        match e {
            Error::FileExists => libc::EEXIST,
            Error::InvalidArgument => libc::EINVAL,
            Error::IsNotDirectory => libc::ENOTDIR,
            Error::NotFound => libc::ENOENT,

            Error::Protocol(reason) => {
                log::error!("Protocol error: {}", reason);
                libc::EIO
            }
        }
    }
}
