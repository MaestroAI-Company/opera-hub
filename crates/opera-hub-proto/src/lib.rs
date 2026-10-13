pub mod invite;

use std::fmt;
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use iroh::{EndpointAddr, EndpointId, RelayUrl};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use url::Url;

pub const PING_ALPN: &[u8] = b"opera/ping/0";
pub const PAIR_ALPN: &[u8] = b"opera/pair/1";
pub const HUB_ALPN: &[u8] = b"opera/hub/1";
pub const ADMIN_ALPN: &[u8] = b"opera/admin/1";
pub const SECRETS_ALPN: &[u8] = b"opera/secrets/1";
pub const PROVIDERS_ALPN: &[u8] = b"opera/providers/1";
pub const SYNC_ALPN: &[u8] = b"opera/sync/1";

//provider configs with their keys and tokens
pub const COLLECTION_PROVIDERS: &str = "providers";
//cloud backup account with its credentials
pub const COLLECTION_ACCOUNTS: &str = "accounts";
pub const SECRET_COLLECTIONS: [&str; 2] = [COLLECTION_PROVIDERS, COLLECTION_ACCOUNTS];
//everyday data under the sync scope
pub const SYNC_COLLECTIONS: [&str; 4] = ["conversations", "messages", "blobs", "settings"];

//mdns service hubs announce on the local network
pub const LAN_SERVICE: &str = "_opera-hub._udp.local.";

//close code for unknown devices or missing scopes
pub const UNAUTHORIZED: u32 = 401;

//plain strings so new scopes need no migration
pub const SCOPE_SYNC: &str = "sync";
pub const SCOPE_SECRETS: &str = "secrets";
pub const SCOPE_PROVIDERS: &str = "providers";
pub const SCOPE_ADMIN: &str = "admin";

pub const KIND_PHONE: &str = "phone";
pub const KIND_DESKTOP: &str = "desktop";
pub const KIND_WEB: &str = "web";

//images travel as blob records
const MAX_FRAME_BYTES: u32 = 32 * 1024 * 1024;
const LINK_PREFIX: &str = "opera://hub/pair";

#[derive(Debug, Serialize, Deserialize)]
pub struct PairRequest {
    pub secret: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum PairResponse {
    Paired { hub_name: String },
    Rejected { reason: PairRejection },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PairRejection {
    Invalid,
    Expired,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubRequest {
    Hello,
    Unpair,
    //any paired device may read the list
    Devices,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubResponse {
    Hello {
        name: String,
        version: String,
        //a newer release the hub can install
        update: Option<String>,
    },
    Unpaired,
    Devices {
        devices: Vec<DeviceInfo>,
    },
}

//managing the hub needs the admin scope
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdminRequest {
    //a pairing code and link for a new device
    Invite,
    Settings,
    SetSettings { settings: HubSettings },
    //install the newest release and restart
    Update,
}

//hub wide toggles, admins change them
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HubSettings {
    //visible to devices on the same network
    pub lan_discovery: bool,
    //paired devices may use this machine's providers
    pub share_providers: bool,
}

impl Default for HubSettings {
    fn default() -> Self {
        Self {
            lan_discovery: true,
            share_providers: true,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdminResponse {
    Invite { code: String, link: String },
    Settings { settings: HubSettings },
    Updating { version: String },
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub scopes: Vec<String>,
    pub paired_at: u64,
    //none when paired from the hub itself
    pub paired_by: Option<String>,
    pub last_seen: Option<u64>,
}

//last writer wins on updated_at then device
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    //unix millis set by the writing device
    pub updated_at: u64,
    pub deleted: bool,
    //json text or none when deleted
    pub payload: Option<String>,
}

//refresh token stays on the hub
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthGrant {
    pub server_id: String,
    pub token_endpoint: String,
    pub client_id: String,
    pub refresh_token: String,
    pub access_token: Option<String>,
    //unix millis
    pub expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessToken {
    pub access_token: String,
    pub expires_at: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SecretsRequest {
    Push {
        collection: String,
        records: Vec<Record>,
    },
    Pull {
        collection: String,
        since: u64,
    },
    PutGrant {
        grant: OAuthGrant,
    },
    AccessToken {
        server_id: String,
    },
    DropGrant {
        server_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SecretsResponse {
    Pushed,
    Records {
        records: Vec<Record>,
        cursor: u64,
        more: bool,
    },
    GrantSaved,
    //none means signing in again
    AccessToken {
        token: Option<AccessToken>,
    },
    GrantDropped,
    Error {
        message: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SyncRequest {
    Push {
        collection: String,
        records: Vec<Record>,
    },
    Pull {
        collection: String,
        since: u64,
    },
    //answers once anything newer than since lands
    Watch {
        since: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SyncResponse {
    Pushed,
    Records {
        records: Vec<Record>,
        cursor: u64,
        more: bool,
    },
    Changed {
        cursor: u64,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInfo {
    pub id: String,
    pub name: String,
    //ollama or openai protocol
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardHead {
    pub provider: String,
    pub method: String,
    //path below the provider base url
    pub path: String,
    pub headers: Vec<(String, String)>,
}

//raw body follows until stream end
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProvidersRequest {
    List,
    Forward { head: ForwardHead },
}

//raw body follows until stream end
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProvidersResponse {
    Providers {
        providers: Vec<ProviderInfo>,
    },
    Response {
        status: u16,
        headers: Vec<(String, String)>,
    },
    Error {
        message: String,
    },
}

//length prefixed json for any stream
pub async fn write_frame<W, T>(writer: &mut W, message: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = serde_json::to_vec(message)?;
    let len = u32::try_from(bytes.len())
        .ok()
        .filter(|len| *len <= MAX_FRAME_BYTES);
    let len = len.ok_or_else(|| anyhow!("message too large"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

//none on a clean end of stream
pub async fn read_frame<R, T>(reader: &mut R) -> Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len = [0u8; 4];
    match reader.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len);
    if len > MAX_FRAME_BYTES {
        bail!("message too large");
    }
    let mut bytes = vec![0u8; len as usize];
    reader.read_exact(&mut bytes).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

pub async fn request<R, W, Req, Resp>(reader: &mut R, writer: &mut W, request: &Req) -> Result<Resp>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    Req: Serialize,
    Resp: DeserializeOwned,
{
    write_frame(writer, request).await?;
    read_frame(reader)
        .await?
        .ok_or_else(|| anyhow!("stream ended before the answer"))
}

pub fn hub_addr(hub: EndpointId, relays: &[RelayUrl]) -> EndpointAddr {
    relays.iter().fold(EndpointAddr::new(hub), |addr, url| {
        addr.with_relay_url(url.clone())
    })
}

//qr code shown by the hub
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairingLink {
    pub hub: EndpointId,
    pub relays: Vec<RelayUrl>,
    pub secret: String,
}

impl fmt::Display for PairingLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut url = Url::parse(LINK_PREFIX).expect("static link prefix");
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("id", &self.hub.to_string());
            query.append_pair("secret", &self.secret);
            for relay in &self.relays {
                query.append_pair("relay", relay.as_str());
            }
        }
        f.write_str(url.as_str())
    }
}

impl FromStr for PairingLink {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let url = Url::parse(s.trim()).context("not a link")?;
        if !url.as_str().starts_with(LINK_PREFIX) {
            bail!("not an opera hub pairing link");
        }
        let (mut hub, mut secret, mut relays) = (None, None, Vec::new());
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "id" => hub = Some(value.parse().context("invalid hub id")?),
                "secret" => secret = Some(value.into_owned()),
                "relay" => relays.push(value.parse().context("invalid relay url")?),
                _ => {}
            }
        }
        Ok(Self {
            hub: hub.ok_or_else(|| anyhow!("missing hub id"))?,
            relays,
            secret: secret.ok_or_else(|| anyhow!("missing secret"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_round_trip() {
        let link = PairingLink {
            hub: iroh::SecretKey::generate().public(),
            relays: vec!["https://relay.example.com/".parse().unwrap()],
            secret: "00112233445566778899aabbccddeeff".into(),
        };
        let parsed: PairingLink = link.to_string().parse().unwrap();
        assert_eq!(parsed, link);
    }

    #[tokio::test]
    async fn frames_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        write_frame(&mut a, &HubRequest::Hello).await.unwrap();
        drop(a);
        assert!(matches!(
            read_frame(&mut b).await.unwrap(),
            Some(HubRequest::Hello)
        ));
        assert!(read_frame::<_, HubRequest>(&mut b).await.unwrap().is_none());
    }

    #[test]
    fn rejects_other_links() {
        assert!(
            "https://example.com/?id=x&secret=y"
                .parse::<PairingLink>()
                .is_err()
        );
        assert!("opera://hub/pair?secret=y".parse::<PairingLink>().is_err());
    }
}
