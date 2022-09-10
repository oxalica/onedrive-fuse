use reqwest::Method;
use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::time::SystemTime;

// FIXME: Customize.
const PAGE_SIZE: usize = 1024;

// Trailing `/` is signiticant for stripping and concatenation.
const API_BASE_URL: &str = "https://graph.microsoft.com/v1.0/";

#[derive(Debug, Clone, Serialize)]
pub struct ApiRequest {
    #[serde(serialize_with = "ser_method")]
    pub method: Method,
    pub url: ApiRelativeUrl,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
}

fn ser_method<S: Serializer>(method: &Method, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(method.as_str())
}

impl ApiRequest {
    pub async fn send<Resp: DeserializeOwned>(
        &self,
        client: &reqwest::Client,
        access_token: &str,
    ) -> reqwest::Result<Resp> {
        let url = (API_BASE_URL.to_owned() + &self.url.0)
            .parse::<reqwest::Url>()
            .unwrap();
        client
            .request(self.method.clone(), url)
            .bearer_auth(access_token)
            .send()
            .await?
            .json()
            .await
    }
}

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

impl DriveItem {
    const SELECT_FIELDS: &'static [&'static str] = &[
        "id",
        "name",
        "size",
        "folder",
        "specialFolder",
        "parentReference",
        "lastModifiedDateTime",
        "createdDateTime",
        "deleted",
    ];
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

pub struct DeltaRequest;

impl DeltaRequest {
    pub fn initial() -> ApiRequest {
        let url = format!(
            "me/drive/root/delta?$top={}&$select={}",
            PAGE_SIZE,
            DriveItem::SELECT_FIELDS.join(","),
        );
        Self::link(ApiRelativeUrl(url))
    }

    pub fn link(link: ApiRelativeUrl) -> ApiRequest {
        ApiRequest {
            method: Method::GET,
            url: link,
            body: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeltaResponse {
    pub value: Vec<DriveItem>,
    #[serde(rename = "@odata.nextLink")]
    pub next_link: Option<ApiRelativeUrl>,
    #[serde(rename = "@odata.deltaLink")]
    pub delta_link: Option<ApiRelativeUrl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRelativeUrl(String);

impl Serialize for ApiRelativeUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ApiRelativeUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.strip_prefix(API_BASE_URL) {
            Some(s) => Ok(Self(s.into())),
            None => Err(D::Error::custom(format!("Invalid API url: {:?}", s))),
        }
    }
}
