use std::path::PathBuf;

pub fn default_credential_path() -> Option<PathBuf> {
    Some(dirs::config_dir()?.join("onedrive-fuse/credential.json"))
}

pub fn default_disk_cache_dir() -> PathBuf {
    std::env::temp_dir().join("onedrive-fuse")
}

pub fn default_database_path() -> Option<PathBuf> {
    Some(dirs::cache_dir()?.join("onedrive-fuse/db.sqlite"))
}
