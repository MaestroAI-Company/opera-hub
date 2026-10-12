use std::sync::Arc;

use iroh::endpoint::{Connection, ConnectionError, RecvStream, SendStream, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayUrl, SecretKey};
use opera_hub_proto::{
    ADMIN_ALPN, AdminRequest, AdminResponse, HUB_ALPN, HubRequest, HubResponse, PAIR_ALPN,
    PairRejection, PairRequest, PairResponse, PairingLink, UNAUTHORIZED, hub_addr, request,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::OnceCell;

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Error)]
#[uniffi(flat_error)]
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
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct PairedHub {
    pub id: String,
    pub name: String,
    pub relays: Vec<String>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct HubStatus {
    pub name: String,
    pub version: String,
    //false while relayed
    pub direct: bool,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct HubDevice {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub scopes: Vec<String>,
    pub paired_at: u64,
    pub paired_by: Option<String>,
    pub last_seen: Option<u64>,
}

#[derive(uniffi::Object)]
pub struct HubClient {
    secret_key: SecretKey,
    //binding needs the tokio runtime
    endpoint: OnceCell<Endpoint>,
}

#[uniffi::export(async_runtime = "tokio")]
impl HubClient {
    //the app creates and stores it
    #[uniffi::constructor]
    pub fn new(secret_key: Vec<u8>) -> Result<Arc<Self>, HubError> {
        let bytes: [u8; 32] = secret_key
            .try_into()
            .map_err(|_| HubError::Internal("device key must be 32 bytes".into()))?;
        Ok(Arc::new(Self {
            secret_key: SecretKey::from_bytes(&bytes),
            endpoint: OnceCell::new(),
        }))
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
        let mut session = self
            .open(hub_addr(link.hub, &link.relays), PAIR_ALPN)
            .await?;
        let response = session
            .request(&PairRequest {
                secret: link.secret,
                name: device_name,
                kind,
            })
            .await?;
        session.close();
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
        let mut session = self.open(paired_addr(&hub)?, HUB_ALPN).await?;
        let response = session.request(&HubRequest::Hello).await?;
        let direct = session
            .conn
            .paths()
            .iter()
            .any(|path| path.is_selected() && path.is_ip());
        session.close();
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
        let mut session = self.open(paired_addr(&hub)?, HUB_ALPN).await?;
        let _: HubResponse = session.request(&HubRequest::Unpair).await?;
        session.close();
        Ok(())
    }

    pub async fn devices(&self, hub: PairedHub) -> Result<Vec<HubDevice>, HubError> {
        let mut session = self.open(paired_addr(&hub)?, ADMIN_ALPN).await?;
        let AdminResponse::Devices { devices } =
            session.request(&AdminRequest::ListDevices).await?;
        session.close();
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

    async fn open(&self, addr: EndpointAddr, alpn: &[u8]) -> Result<Session, HubError> {
        let conn = self
            .endpoint()
            .await?
            .connect(addr, alpn)
            .await
            .map_err(unreachable)?;
        let (send, recv) = conn.open_bi().await.map_err(unreachable)?;
        Ok(Session { conn, send, recv })
    }
}

//one protocol stream over one connection
struct Session {
    conn: Connection,
    send: SendStream,
    recv: RecvStream,
}

impl Session {
    async fn request<Req: Serialize, Resp: DeserializeOwned>(
        &mut self,
        req: &Req,
    ) -> Result<Resp, HubError> {
        request(&mut self.recv, &mut self.send, req)
            .await
            .map_err(|e| match self.conn.close_reason() {
                Some(ConnectionError::ApplicationClosed(close))
                    if u64::from(close.error_code) == u64::from(UNAUTHORIZED) =>
                {
                    HubError::Unauthorized
                }
                _ => unreachable(e),
            })
    }

    fn close(mut self) {
        let _ = self.send.finish();
        self.conn.close(0u32.into(), b"done");
    }
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

fn unreachable(e: impl std::fmt::Display) -> HubError {
    HubError::Unreachable(e.to_string())
}
