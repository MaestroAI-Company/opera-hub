use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use opera_hub_proto::{
    AccessToken, COLLECTION_PROVIDERS, SCOPE_SECRETS, SecretsRequest, SecretsResponse, read_frame,
    write_frame,
};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::warn;

use crate::db::Db;
use crate::protocols::{Access, Peer, Protocol};
use crate::store::unix_now;

const PULL_LIMIT: usize = 200;
//renew early before expiry
const EXPIRY_MARGIN_MS: u64 = 60_000;

pub fn http_client() -> Result<reqwest::Client> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .connect_timeout(Duration::from_secs(10))
        .build()?)
}

//provider configs and the oauth token broker
#[derive(Clone)]
pub struct Secrets {
    pub db: Arc<Db>,
    pub http: reqwest::Client,
    //serialized so rotating tokens never fork
    pub refreshing: Arc<tokio::sync::Mutex<()>>,
}

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secrets")
    }
}

impl Protocol for Secrets {
    const ACCESS: Access = Access::Scope(SCOPE_SECRETS);

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match self.handle(&peer, request).await {
                Ok(response) => response,
                Err(e) => SecretsResponse::Error {
                    message: format!("{e:#}"),
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Secrets {
    async fn handle(&self, peer: &Peer, request: SecretsRequest) -> Result<SecretsResponse> {
        Ok(match request {
            SecretsRequest::Push {
                collection,
                records,
            } => {
                check_collection(&collection)?;
                self.db.push(&collection, &peer.id, &records)?;
                SecretsResponse::Pushed
            }
            SecretsRequest::Pull { collection, since } => {
                check_collection(&collection)?;
                let (records, cursor, more) = self.db.pull(&collection, since, PULL_LIMIT)?;
                SecretsResponse::Records {
                    records,
                    cursor,
                    more,
                }
            }
            SecretsRequest::PutGrant { grant } => {
                check_endpoint(&grant.token_endpoint)?;
                self.db.put_grant(&grant)?;
                SecretsResponse::GrantSaved
            }
            SecretsRequest::AccessToken { server_id } => SecretsResponse::AccessToken {
                token: self.access_token(&server_id).await?,
            },
            SecretsRequest::DropGrant { server_id } => {
                self.db.drop_grant(&server_id)?;
                SecretsResponse::GrantDropped
            }
        })
    }

    async fn access_token(&self, server_id: &str) -> Result<Option<AccessToken>> {
        let _guard = self.refreshing.lock().await;
        let Some(mut grant) = self.db.grant(server_id)? else {
            return Ok(None);
        };
        if let Some(token) = &grant.access_token
            && grant.expires_at > unix_now() * 1000 + EXPIRY_MARGIN_MS
        {
            return Ok(Some(AccessToken {
                access_token: token.clone(),
                expires_at: grant.expires_at,
            }));
        }

        let response = self
            .http
            .post(&grant.token_endpoint)
            .header("Accept", "application/json")
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", grant.refresh_token.as_str()),
                ("client_id", grant.client_id.as_str()),
            ])
            .send()
            .await?;
        //refused refresh means signing in again
        if response.status().is_client_error() {
            warn!(
                "refresh refused for mcp server {server_id}: {}",
                response.status()
            );
            self.db.drop_grant(server_id)?;
            return Ok(None);
        }
        let response = response.error_for_status()?;
        let doc: TokenResponse = serde_json::from_slice(&response.bytes().await?)?;

        grant.expires_at = unix_now() * 1000 + doc.expires_in.unwrap_or(3600) * 1000;
        grant.access_token = Some(doc.access_token.clone());
        if let Some(refresh) = doc.refresh_token {
            grant.refresh_token = refresh;
        }
        self.db.put_grant(&grant)?;
        Ok(Some(AccessToken {
            access_token: doc.access_token,
            expires_at: grant.expires_at,
        }))
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
    refresh_token: Option<String>,
}

fn check_collection(collection: &str) -> Result<()> {
    if collection != COLLECTION_PROVIDERS {
        bail!("unknown collection {collection}");
    }
    Ok(())
}

//https only except localhost
fn check_endpoint(endpoint: &str) -> Result<()> {
    let url = reqwest::Url::parse(endpoint)?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if url.scheme() != "https" && !(url.scheme() == "http" && local) {
        return Err(anyhow!("token endpoint must use https"));
    }
    Ok(())
}
