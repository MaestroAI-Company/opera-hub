#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
mod proxy;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod web;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::{Connection, ConnectionError, RecvStream, SendStream, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayUrl, SecretKey};
use opera_hub_proto::{
    ForwardHead, HUB_ALPN, HubRequest, HubResponse, OAuthGrant, PAIR_ALPN, PROVIDERS_ALPN,
    PairRejection, PairRequest, PairResponse, PairingLink, ProvidersRequest, ProvidersResponse,
    Record, SECRET_COLLECTIONS, SECRETS_ALPN, SYNC_ALPN, SecretsRequest, SecretsResponse,
    SyncRequest, SyncResponse, UNAUTHORIZED, hub_addr, read_frame, request, write_frame,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::{Mutex, OnceCell};

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
uniffi::setup_scaffolding!();

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
//slowest relay upload still allowed to finish
const MIN_UPLOAD_BYTES_PER_SEC: usize = 32 * 1024;

#[derive(Debug, thiserror::Error)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Error),
    uniffi(flat_error)
)]
pub enum HubError {
    #[error("invalid pairing link: {0}")]
    InvalidLink(String),
    #[error("pairing code not recognized")]
    PairingRejected,
    #[error("pairing code expired")]
    PairingExpired,
    #[error("this device is not paired with the hub")]
    Unauthorized,
    #[error("hub unreachable: {0}")]
    Unreachable(String),
    #[error("the hub is too old for this app, update it")]
    Outdated,
    #[error("{0}")]
    Internal(String),
}

//desktop and web read the code like android
impl Serialize for HubError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let code = match self {
            HubError::InvalidLink(_) => "InvalidLink",
            HubError::PairingRejected => "PairingRejected",
            HubError::PairingExpired => "PairingExpired",
            HubError::Unauthorized => "Unauthorized",
            HubError::Unreachable(_) => "Unreachable",
            HubError::Outdated => "Outdated",
            HubError::Internal(_) => "Internal",
        };
        let mut error = serializer.serialize_struct("HubError", 2)?;
        error.serialize_field("code", code)?;
        error.serialize_field("message", &self.to_string())?;
        error.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct PairedHub {
    pub id: String,
    pub name: String,
    pub relays: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubStatus {
    pub name: String,
    pub version: String,
    //false while relayed
    pub direct: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubDevice {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub scopes: Vec<String>,
    pub paired_at: u64,
    pub paired_by: Option<String>,
    pub last_seen: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct SyncRecord {
    pub id: String,
    pub updated_at: u64,
    pub deleted: bool,
    pub payload: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct PullPage {
    pub records: Vec<SyncRecord>,
    pub cursor: u64,
    pub more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpGrant {
    pub server_id: String,
    pub token_endpoint: String,
    pub client_id: String,
    pub refresh_token: String,
    pub access_token: Option<String>,
    pub expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpAccessToken {
    pub access_token: String,
    pub expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubProvider {
    pub id: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct ProxyInfo {
    pub port: u16,
    pub token: String,
}

#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Object)
)]
pub struct HubClient {
    secret_key: SecretKey,
    //binding needs the tokio runtime
    endpoint: OnceCell<Endpoint>,
    //kept open so paths can turn direct
    connections: Mutex<HashMap<(String, &'static [u8]), Connection>>,
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    proxy: Mutex<Option<ProxyInfo>>,
}

#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    uniffi::export(async_runtime = "tokio")
)]
impl HubClient {
    //the app creates and stores it
    #[cfg_attr(
        not(all(target_family = "wasm", target_os = "unknown")),
        uniffi::constructor
    )]
    pub fn new(secret_key: Vec<u8>) -> Result<Arc<Self>, HubError> {
        let bytes: [u8; 32] = secret_key
            .try_into()
            .map_err(|_| HubError::Internal("device key must be 32 bytes".into()))?;
        Ok(Arc::new(Self {
            secret_key: SecretKey::from_bytes(&bytes),
            endpoint: OnceCell::new(),
            connections: Mutex::new(HashMap::new()),
            #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
            proxy: Mutex::new(None),
        }))
    }

    //the id the hub lists for this device
    pub fn device_id(&self) -> String {
        self.secret_key.public().to_string()
    }

    pub async fn pair(
        &self,
        link: String,
        device_name: String,
        kind: String,
    ) -> Result<PairedHub, HubError> {
        let link: PairingLink = link
            .parse()
            .map_err(|e| HubError::InvalidLink(format!("{e:#}")))?;
        let conn = self
            .connect(hub_addr(link.hub, &link.relays), PAIR_ALPN)
            .await?;
        let (send, recv) = conn.open_bi().await.map_err(unreachable)?;
        let mut stream = Stream {
            conn: conn.clone(),
            send,
            recv,
        };
        let response = stream
            .request(&PairRequest {
                secret: link.secret,
                name: device_name,
                kind,
            })
            .await?;
        stream.finish();
        conn.close(0u32.into(), b"done");
        match response {
            PairResponse::Paired { hub_name } => Ok(PairedHub {
                id: link.hub.to_string(),
                name: hub_name,
                relays: link.relays.iter().map(|url| url.to_string()).collect(),
            }),
            PairResponse::Rejected {
                reason: PairRejection::Invalid,
            } => Err(HubError::PairingRejected),
            PairResponse::Rejected {
                reason: PairRejection::Expired,
            } => Err(HubError::PairingExpired),
        }
    }

    pub async fn status(&self, hub: PairedHub) -> Result<HubStatus, HubError> {
        let mut stream = self.stream(&hub, HUB_ALPN).await?;
        let response = stream.request(&HubRequest::Hello).await?;
        let direct = stream
            .conn
            .paths()
            .iter()
            .any(|path| path.is_selected() && path.is_ip());
        stream.finish();
        match response {
            HubResponse::Hello { name, version } => Ok(HubStatus {
                name,
                version,
                direct,
            }),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn unpair(&self, hub: PairedHub) -> Result<(), HubError> {
        let mut stream = self.stream(&hub, HUB_ALPN).await?;
        let _: HubResponse = stream.request(&HubRequest::Unpair).await?;
        stream.finish();
        //cached sessions would now be refused
        let mut connections = self.connections.lock().await;
        connections.retain(|(id, _), conn| {
            if *id == hub.id {
                conn.close(0u32.into(), b"unpaired");
            }
            *id != hub.id
        });
        Ok(())
    }

    pub async fn devices(&self, hub: PairedHub) -> Result<Vec<HubDevice>, HubError> {
        let mut stream = self.stream(&hub, HUB_ALPN).await?;
        let response = stream.request(&HubRequest::Devices).await?;
        stream.finish();
        let HubResponse::Devices { devices } = response else {
            return Err(HubError::Internal(format!(
                "unexpected answer {response:?}"
            )));
        };
        Ok(devices
            .into_iter()
            .map(|d| HubDevice {
                id: d.id,
                name: d.name,
                kind: d.kind,
                scopes: d.scopes,
                paired_at: d.paired_at,
                paired_by: d.paired_by,
                last_seen: d.last_seen,
            })
            .collect())
    }

    pub async fn push(
        &self,
        hub: PairedHub,
        collection: String,
        records: Vec<SyncRecord>,
    ) -> Result<(), HubError> {
        let records = records
            .into_iter()
            .map(|r| Record {
                id: r.id,
                updated_at: r.updated_at,
                deleted: r.deleted,
                payload: r.payload,
            })
            .collect();
        //provider and account secrets need their own scope
        if SECRET_COLLECTIONS.contains(&collection.as_str()) {
            let request = SecretsRequest::Push {
                collection,
                records,
            };
            return match self.secrets(&hub, request).await? {
                SecretsResponse::Pushed => Ok(()),
                other => Err(unexpected(other)),
            };
        }
        let request = SyncRequest::Push {
            collection,
            records,
        };
        match self.data(&hub, request).await? {
            SyncResponse::Pushed => Ok(()),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn pull(
        &self,
        hub: PairedHub,
        collection: String,
        since: u64,
    ) -> Result<PullPage, HubError> {
        let (records, cursor, more) = if SECRET_COLLECTIONS.contains(&collection.as_str()) {
            match self
                .secrets(&hub, SecretsRequest::Pull { collection, since })
                .await?
            {
                SecretsResponse::Records {
                    records,
                    cursor,
                    more,
                } => (records, cursor, more),
                other => return Err(unexpected(other)),
            }
        } else {
            match self
                .data(&hub, SyncRequest::Pull { collection, since })
                .await?
            {
                SyncResponse::Records {
                    records,
                    cursor,
                    more,
                } => (records, cursor, more),
                other => return Err(HubError::Internal(format!("unexpected answer {other:?}"))),
            }
        };
        Ok(PullPage {
            records: records
                .into_iter()
                .map(|r| SyncRecord {
                    id: r.id,
                    updated_at: r.updated_at,
                    deleted: r.deleted,
                    payload: r.payload,
                })
                .collect(),
            cursor,
            more,
        })
    }

    //waits until the hub has anything newer than since
    pub async fn watch(&self, hub: PairedHub, since: u64) -> Result<u64, HubError> {
        match self.data(&hub, SyncRequest::Watch { since }).await? {
            SyncResponse::Changed { cursor } => Ok(cursor),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn put_grant(&self, hub: PairedHub, grant: McpGrant) -> Result<(), HubError> {
        let grant = OAuthGrant {
            server_id: grant.server_id,
            token_endpoint: grant.token_endpoint,
            client_id: grant.client_id,
            refresh_token: grant.refresh_token,
            access_token: grant.access_token,
            expires_at: grant.expires_at,
        };
        match self
            .secrets(&hub, SecretsRequest::PutGrant { grant })
            .await?
        {
            SecretsResponse::GrantSaved => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    //none means signing in again
    pub async fn access_token(
        &self,
        hub: PairedHub,
        server_id: String,
    ) -> Result<Option<McpAccessToken>, HubError> {
        match self
            .secrets(&hub, SecretsRequest::AccessToken { server_id })
            .await?
        {
            SecretsResponse::AccessToken { token } => Ok(token.map(|t| McpAccessToken {
                access_token: t.access_token,
                expires_at: t.expires_at,
            })),
            other => Err(unexpected(other)),
        }
    }

    pub async fn drop_grant(&self, hub: PairedHub, server_id: String) -> Result<(), HubError> {
        match self
            .secrets(&hub, SecretsRequest::DropGrant { server_id })
            .await?
        {
            SecretsResponse::GrantDropped => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn providers(&self, hub: PairedHub) -> Result<Vec<HubProvider>, HubError> {
        let mut stream = self.stream(&hub, PROVIDERS_ALPN).await?;
        let response = stream.request(&ProvidersRequest::List).await?;
        stream.finish();
        match response {
            ProvidersResponse::Providers { providers } => Ok(providers
                .into_iter()
                .map(|p| HubProvider {
                    id: p.id,
                    name: p.name,
                    kind: p.kind,
                })
                .collect()),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    //local entry point for providers
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    pub async fn start_proxy(self: Arc<Self>, hub: PairedHub) -> Result<ProxyInfo, HubError> {
        let mut proxy = self.proxy.lock().await;
        if let Some(info) = proxy.as_ref() {
            return Ok(info.clone());
        }
        let info = proxy::start(self.clone(), hub).await?;
        *proxy = Some(info.clone());
        Ok(info)
    }
}

impl HubClient {
    async fn endpoint(&self) -> Result<&Endpoint, HubError> {
        self.endpoint
            .get_or_try_init(|| async {
                Endpoint::builder(presets::N0)
                    .secret_key(self.secret_key.clone())
                    .bind()
                    .await
                    .map_err(unreachable)
            })
            .await
    }

    async fn connect(&self, addr: EndpointAddr, alpn: &[u8]) -> Result<Connection, HubError> {
        let endpoint = self.endpoint().await?;
        let conn = within(CONNECT_TIMEOUT, endpoint.connect(addr, alpn)).await?;
        conn.map_err(|e| {
            //tls alert for an alpn the hub does not serve
            if e.to_string().contains("error 120") {
                HubError::Outdated
            } else {
                unreachable(e)
            }
        })
    }

    //reconnects once if the connection died
    async fn stream(&self, hub: &PairedHub, alpn: &'static [u8]) -> Result<Stream, HubError> {
        let key = (hub.id.clone(), alpn);
        for _ in 0..2 {
            let conn = {
                let mut connections = self.connections.lock().await;
                match connections.get(&key) {
                    Some(conn) if conn.close_reason().is_none() => conn.clone(),
                    _ => {
                        let conn = self.connect(paired_addr(hub)?, alpn).await?;
                        connections.insert(key.clone(), conn.clone());
                        conn
                    }
                }
            };
            match conn.open_bi().await {
                Ok((send, recv)) => return Ok(Stream { conn, send, recv }),
                Err(_) if refused(&conn) => return Err(HubError::Unauthorized),
                Err(_) => continue,
            }
        }
        Err(HubError::Unreachable("connection keeps closing".into()))
    }

    //one provider request through the hub
    pub(crate) async fn forward(
        &self,
        hub: &PairedHub,
        head: ForwardHead,
        body: &[u8],
    ) -> Result<Forwarded, HubError> {
        let mut stream = self.stream(hub, PROVIDERS_ALPN).await?;
        write_frame(&mut stream.send, &ProvidersRequest::Forward { head })
            .await
            .map_err(unreachable)?;
        stream.send.write_all(body).await.map_err(unreachable)?;
        stream.send.finish().map_err(unreachable)?;
        let response = read_frame(&mut stream.recv)
            .await
            .map_err(unreachable)?
            .ok_or_else(|| HubError::Unreachable("hub closed the stream".into()))?;
        match response {
            ProvidersResponse::Response { status, headers } => Ok(Forwarded::Response {
                status,
                headers,
                body: stream.recv,
            }),
            ProvidersResponse::Error { message } => Ok(Forwarded::Refused(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    async fn data(&self, hub: &PairedHub, request: SyncRequest) -> Result<SyncResponse, HubError> {
        let mut stream = self.stream(hub, SYNC_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            SyncResponse::Error { message } => Err(HubError::Internal(message)),
            other => Ok(other),
        }
    }

    async fn secrets(
        &self,
        hub: &PairedHub,
        request: SecretsRequest,
    ) -> Result<SecretsResponse, HubError> {
        let mut stream = self.stream(hub, SECRETS_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            SecretsResponse::Error { message } => Err(HubError::Internal(message)),
            other => Ok(other),
        }
    }
}

//the body streams as the provider writes it
pub(crate) enum Forwarded {
    Response {
        status: u16,
        headers: Vec<(String, String)>,
        body: RecvStream,
    },
    Refused(String),
}

//one protocol session
struct Stream {
    conn: Connection,
    send: SendStream,
    recv: RecvStream,
}

impl Stream {
    async fn request<Req: Serialize, Resp: DeserializeOwned>(
        &mut self,
        req: &Req,
    ) -> Result<Resp, HubError> {
        //big uploads get time while a silent hub fails fast
        let size = serde_json::to_vec(req).map_or(0, |bytes| bytes.len());
        let limit = REQUEST_TIMEOUT + Duration::from_secs((size / MIN_UPLOAD_BYTES_PER_SEC) as u64);
        within(limit, request(&mut self.recv, &mut self.send, req))
            .await?
            .map_err(|e| {
                if refused(&self.conn) {
                    HubError::Unauthorized
                } else {
                    unreachable(e)
                }
            })
    }

    fn finish(mut self) {
        let _ = self.send.finish();
    }
}

fn refused(conn: &Connection) -> bool {
    matches!(
        conn.close_reason(),
        Some(ConnectionError::ApplicationClosed(close))
            if u64::from(close.error_code) == u64::from(UNAUTHORIZED)
    )
}

fn paired_addr(hub: &PairedHub) -> Result<EndpointAddr, HubError> {
    let id: EndpointId = hub
        .id
        .parse()
        .map_err(|_| HubError::Internal("invalid hub id".into()))?;
    let relays = hub
        .relays
        .iter()
        .map(|url| url.parse::<RelayUrl>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| HubError::Internal("invalid relay url".into()))?;
    Ok(hub_addr(id, &relays))
}

fn unexpected(response: SecretsResponse) -> HubError {
    HubError::Internal(format!("unexpected answer {response:?}"))
}

//a silent hub must never hang the app
async fn within<T>(limit: Duration, future: impl Future<Output = T>) -> Result<T, HubError> {
    n0_future::time::timeout(limit, future)
        .await
        .map_err(|_| HubError::Unreachable("the hub did not answer in time".into()))
}

fn unreachable(e: impl std::fmt::Display) -> HubError {
    HubError::Unreachable(e.to_string())
}
