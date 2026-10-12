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

//close code for unknown devices or missing scopes
pub const UNAUTHORIZED: u32 = 401;

//plain strings so new scopes need no migration
pub const SCOPE_SYNC: &str = "sync";
pub const SCOPE_SECRETS: &str = "secrets";
pub const SCOPE_PROVIDERS: &str = "providers";
pub const SCOPE_ADMIN: &str = "admin";

pub const KIND_PHONE: &str = "phone";
pub const KIND_DESKTOP: &str = "desktop";

const MAX_FRAME_BYTES: u32 = 64 * 1024;
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
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubResponse {
    Hello { name: String, version: String },
    Unpaired,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdminRequest {
    ListDevices,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdminResponse {
    Devices { devices: Vec<DeviceInfo> },
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
