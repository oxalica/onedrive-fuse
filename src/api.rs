use serde::Deserialize;
use std::time::SystemTime;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DriveItem {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub folder: Option<Empty>,
    pub special_folder: Option<Empty>,
    pub parent_reference: Option<ParentReference>,
    #[serde(with = "humantime_serde", default = "default_time")]
    pub last_modified_date_time: SystemTime,
    #[serde(with = "humantime_serde", default = "default_time")]
    pub created_date_time: SystemTime,
    pub deleted: Option<Empty>,
}

#[derive(Debug, Deserialize)]
pub struct Empty {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentReference {
    pub id: String,
}

fn default_time() -> SystemTime {
    SystemTime::UNIX_EPOCH
}
