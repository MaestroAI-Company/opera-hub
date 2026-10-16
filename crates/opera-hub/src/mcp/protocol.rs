//opera/mcp/1, admins manage hosted servers
use std::sync::Arc;

use anyhow::Result;
use opera_hub_proto::{McpRequest, McpResponse, SCOPE_ADMIN, read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};

use super::Mcp;
use crate::protocols::{Access, Peer, Protocol};

#[derive(Debug, Clone)]
pub struct McpAdmin {
    pub mcp: Arc<Mcp>,
}

impl Protocol for McpAdmin {
    const ACCESS: Access = Access::Scope(SCOPE_ADMIN);

    async fn serve<R, W>(&self, _peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let Some(request) = read_frame(&mut reader).await? else {
            return Ok(());
        };
        let response = self
            .handle(request)
            .await
            .unwrap_or_else(|e| McpResponse::Error {
                message: format!("{e:#}"),
            });
        write_frame(&mut writer, &response).await
    }
}

impl McpAdmin {
    async fn handle(&self, request: McpRequest) -> Result<McpResponse> {
        let mcp = &self.mcp;
        let status = || -> Result<McpResponse> {
            Ok(McpResponse::Status {
                isolation: mcp.isolation().into(),
                servers: mcp.status()?,
            })
        };
        Ok(match request {
            McpRequest::Catalog => McpResponse::Listings {
                listings: mcp.catalog(),
            },
            McpRequest::Search { query } => McpResponse::Listings {
                listings: mcp.search(&query).await?,
            },
            McpRequest::Prepare { listing } => McpResponse::Listing {
                listing: Box::new(mcp.prepare(listing).await?),
            },
            McpRequest::Install { listing, values } => McpResponse::Installed {
                id: mcp.install(listing, values)?,
            },
            McpRequest::Configure { id, values } => {
                mcp.configure(&id, values)?;
                status()?
            }
            McpRequest::Enable { id, on } => {
                mcp.enable(&id, on)?;
                status()?
            }
            McpRequest::Restart { id } => {
                mcp.restart(&id)?;
                status()?
            }
            McpRequest::Update { id } => {
                mcp.update(&id).await?;
                status()?
            }
            McpRequest::Remove { id } => {
                mcp.remove(&id).await?;
                status()?
            }
            McpRequest::Status => status()?,
            McpRequest::Logs { id } => McpResponse::Logs {
                lines: mcp.logs(&id)?,
            },
            McpRequest::Folders { path } => {
                let (path, parent, folders) = mcp.folders(path.as_deref())?;
                McpResponse::Folders {
                    path,
                    parent,
                    folders,
                }
            }
            McpRequest::Registries => McpResponse::Registries {
                urls: mcp.registries(),
            },
            McpRequest::SetRegistries { urls } => {
                mcp.set_registries(urls)?;
                McpResponse::Registries {
                    urls: mcp.registries(),
                }
            }
        })
    }
}
