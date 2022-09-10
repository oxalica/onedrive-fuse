// FIXME
#![allow(clippy::new_ret_no_self)]
use reqwest::{Method, StatusCode};
use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_aux::field_attributes::deserialize_number_from_string;
use serde_aux::serde_introspection::serde_introspect;
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
        let mut req = client
            .request(self.method.clone(), url)
            .bearer_auth(access_token);
        if let Some(body) = &self.body {
            req = req.json(body);
        }
        req.send().await?.json().await
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DriveItem {
    pub id: String,
    pub name: String,
    pub size: Option<u64>,
    pub folder: Option<Empty>,
    pub root: Option<Empty>,
    pub special_folder: Option<Empty>,
    pub parent_reference: Option<ParentReference>,
    #[serde(with = "humantime_serde", default = "default_time")]
    pub last_modified_date_time: SystemTime,
    #[serde(with = "humantime_serde", default = "default_time")]
    pub created_date_time: SystemTime,
    pub deleted: Option<Empty>,
}

impl DriveItem {
    fn select_fields() -> &'static [&'static str] {
        serde_introspect::<Self>()
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
            DriveItem::select_fields().join(","),
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

// https://docs.microsoft.com/en-us/graph/delta-query-overview?context=graph%2Fapi%2F1.0&view=graph-rest-1.0
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

// https://docs.microsoft.com/en-us/graph/api/driveitem-post-children?view=graph-rest-1.0&tabs=http
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDir {
    name: String,
    folder: Empty,
    #[serde(rename = "@microsoft.graph.conflictBehavior")]
    conflict_behavior: ConflictBehavior,
}

// https://docs.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0#instance-attributes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
enum ConflictBehavior {
    Replace,
}

impl CreateDir {
    pub fn new(path: &str) -> ApiRequest {
        let (parent_path, child_name) = path.rsplit_once('/').unwrap();
        let url = if parent_path.is_empty() {
            "me/drive/root/children".to_owned()
        } else {
            format!("me/drive/root:{}:/children", parent_path)
        };
        let body = Self {
            name: child_name.into(),
            folder: Empty {},
            conflict_behavior: ConflictBehavior::Replace,
        };
        ApiRequest {
            method: Method::POST,
            url: ApiRelativeUrl(url),
            body: Some(serde_json::to_value(body).unwrap()),
        }
    }
}

// https://docs.microsoft.com/en-us/graph/json-batching?context=graph%2Fapi%2F1.0&view=graph-rest-1.0#request-format
#[derive(Debug, Serialize)]
pub struct BatchRequest {
    requests: Vec<BatchRequestEntry>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchRequestEntry {
    id: usize,
    depends_on: Option<(usize,)>,
    method: String,
    url: ApiRelativeUrl,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<Headers>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct Headers {
    content_type: &'static str,
}

impl BatchRequest {
    // https://docs.microsoft.com/en-us/graph/json-batching?context=graph%2Fapi%2F1.0&view=graph-rest-1.0#batch-size-limitations
    pub const MAX_LEN: usize = 20;

    pub fn sequential(requests: impl IntoIterator<Item = ApiRequest>) -> ApiRequest {
        let url = ApiRelativeUrl("$batch".into());
        let requests = requests
            .into_iter()
            .enumerate()
            .map(|(id, req)| BatchRequestEntry {
                id,
                depends_on: if id == 0 { None } else { Some((id - 1,)) },
                method: req.method.to_string(),
                url: req.url,
                headers: req.body.as_ref().map(|_| Headers {
                    content_type: mime::APPLICATION_JSON.as_ref(),
                }),
                body: req.body,
            })
            .collect::<Vec<_>>();
        assert!(requests.len() <= Self::MAX_LEN);
        let body = BatchRequest { requests };
        ApiRequest {
            method: Method::POST,
            url,
            body: Some(serde_json::to_value(body).unwrap()),
        }
    }
}

// https://docs.microsoft.com/en-us/graph/json-batching?context=graph%2Fapi%2F1.0&view=graph-rest-1.0#response-format
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchResponse {
    pub responses: Vec<BatchResponseEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchResponseEntry {
    // Workaround: the API responds string ids while numeric ids are given.
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub id: usize,
    #[serde(deserialize_with = "de_status")]
    pub status: StatusCode,
    #[serde(default)]
    pub body: Option<serde_json::Value>,
}

pub fn de_status<'de, D>(de: D) -> Result<StatusCode, D::Error>
where
    D: Deserializer<'de>,
{
    StatusCode::from_u16(u16::deserialize(de)?).map_err(D::Error::custom)
}
