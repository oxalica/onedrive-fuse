mod fuse;
mod vfs;

pub use self::fuse::Filesystem;

type CResult<T> = Result<T, libc::c_int>;
