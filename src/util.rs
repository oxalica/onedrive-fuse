use serde::de::{Deserialize, Deserializer};
use std::time::Duration;

pub fn de_duration_sec<'de, D>(de: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    u64::deserialize(de).map(Duration::from_secs)
}
