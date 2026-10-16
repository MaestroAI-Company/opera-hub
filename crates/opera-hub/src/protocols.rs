//transport agnostic hub protocols

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use opera_hub_proto::{
    AdminRequest, AdminResponse, HubRequest, HubResponse, PairRequest, PairResponse, SCOPE_ADMIN,
    clean_device_name, read_frame, write_frame,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

use crate::store::{Device, Store, default_scopes, unix_now};

const MAX_DEVICE_NAME: usize = 64;

//proven by the transport
#[derive(Debug, Clone)]
pub struct Peer {
    pub id: String,
}

impl Peer {
    fn short(&self) -> &str {
        &self.id[..self.id.len().min(10)]
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Access {
    //no device data so open
    Public,
    Paired,
    Scope(&'static str),
}

pub trait Protocol: Clone + Send + Sync + 'static {
    const ACCESS: Access;

    fn serve<R, W>(
        &self,
        peer: Peer,
        reader: R,
        writer: W,
    ) -> impl Future<Output = Result<()>> + Send
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send;
}

//checked again for every stream, a removed device stops at once
pub fn still_allowed(store: &Store, peer: &Peer, access: Access) -> Result<bool> {
    let device = store.device(&peer.id)?;
    Ok(match (access, &device) {
        (Access::Public, _) => true,
        (_, None) => false,
        (Access::Paired, Some(_)) => true,
        (Access::Scope(scope), Some(device)) => device.scopes.iter().any(|s| s == scope),
    })
}

//deny by default before any serve
pub fn admit(store: &Store, peer: &Peer, access: Access) -> Result<bool> {
    let device = store.device(&peer.id)?;
    let allowed = match (access, &device) {
        (Access::Public, _) => true,
        (_, None) => false,
        (Access::Paired, Some(_)) => true,
        (Access::Scope(scope), Some(device)) => device.scopes.iter().any(|s| s == scope),
    };
    if allowed && device.is_some() {
        store.touch(&peer.id)?;
    }
    if !allowed {
        warn!("refused {} on {access:?}", peer.short());
    }
    Ok(allowed)
}

//reachability check
#[derive(Debug, Clone)]
pub struct Ping;

impl Protocol for Ping {
    const ACCESS: Access = Access::Public;

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        info!("ping from {}", peer.short());
        tokio::io::copy(&mut reader, &mut writer).await?;
        Ok(())
    }
}

//allowlists devices with a valid secret
#[derive(Debug, Clone)]
pub struct Pair {
    pub store: Arc<Store>,
    pub name: Arc<str>,
}

impl Protocol for Pair {
    const ACCESS: Access = Access::Public;

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let Some(request) = read_frame::<_, PairRequest>(&mut reader).await? else {
            return Ok(());
        };
        let kind: String = request.kind.chars().take(MAX_DEVICE_NAME).collect();
        let device = Device {
            id: peer.id.clone(),
            name: request.name.chars().take(MAX_DEVICE_NAME).collect(),
            scopes: default_scopes(&kind),
            kind,
            paired_at: unix_now(),
            //qr codes come from the hub owner
            paired_by: None,
            last_seen: Some(unix_now()),
            //only a hash fits
            machine: request
                .machine
                .filter(|tag| tag.len() == 64 && tag.bytes().all(|b| b.is_ascii_hexdigit())),
        };
        let label = format!("{} ({})", device.name, peer.short());
        let response = match self.store.finish_pairing(&request.secret, device)? {
            Ok(()) => {
                info!("paired {label}");
                PairResponse::Paired {
                    hub_name: self.name.to_string(),
                }
            }
            Err(reason) => {
                warn!("refused pairing from {label}: {reason:?}");
                PairResponse::Rejected { reason }
            }
        };
        write_frame(&mut writer, &response).await
    }
}

//requests open to any paired device
#[derive(Debug, Clone)]
pub struct Session {
    pub store: Arc<Store>,
    pub name: Arc<str>,
    pub latest: tokio::sync::watch::Receiver<Option<String>>,
    pub usage: Arc<crate::usage::Usage>,
}

impl Protocol for Session {
    const ACCESS: Access = Access::Paired;

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match request {
                HubRequest::Hello => {
                    let load = self.usage.hub();
                    HubResponse::Hello {
                        name: self.name.to_string(),
                        version: env!("CARGO_PKG_VERSION").into(),
                        update: self.latest.borrow().clone(),
                        usage: Some(opera_hub_proto::HubUsage {
                            cpu: load.cpu,
                            memory: load.memory,
                        }),
                    }
                }
                HubRequest::Devices => HubResponse::Devices {
                    devices: self.store.devices()?,
                },
                HubRequest::Rename { name } => match clean_device_name(&name) {
                    Some(name) => {
                        self.store.rename_device(&peer.id, &name)?;
                        HubResponse::Renamed
                    }
                    None => HubResponse::Error {
                        message: "a device needs a name".into(),
                    },
                },
                HubRequest::Unpair => {
                    self.store.remove_device(&peer.id)?;
                    info!("{} unpaired itself", peer.short());
                    write_frame(&mut writer, &HubResponse::Unpaired).await?;
                    return Ok(());
                }
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Admin {
    pub store: Arc<Store>,
    pub data_dir: std::path::PathBuf,
    pub settings: Arc<crate::settings::Settings>,
    pub http: reqwest::Client,
    pub latest: tokio::sync::watch::Receiver<Option<String>>,
}

impl std::fmt::Debug for Admin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Admin")
    }
}

impl Protocol for Admin {
    const ACCESS: Access = Access::Scope(SCOPE_ADMIN);

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match request {
                AdminRequest::Invite => match crate::invite::open(&self.data_dir).await {
                    Ok(invite) => {
                        info!("{} opened a pairing code", peer.short());
                        AdminResponse::Invite {
                            code: invite.code,
                            link: invite.link.to_string(),
                        }
                    }
                    Err(e) => AdminResponse::Error {
                        message: format!("{e:#}"),
                    },
                },
                AdminRequest::Settings => AdminResponse::Settings {
                    settings: self.settings.get(),
                },
                AdminRequest::Update => match self.update().await {
                    Ok(version) => {
                        info!("{} asked for opera hub {version}", peer.short());
                        write_frame(&mut writer, &AdminResponse::Updating { version }).await?;
                        restart_soon();
                        return Ok(());
                    }
                    Err(e) => AdminResponse::Error {
                        message: format!("{e:#}"),
                    },
                },
                AdminRequest::RenameDevice { id, name } => match clean_device_name(&name) {
                    Some(name) if self.store.rename_device(&id, &name)? => {
                        info!("{} renamed a device to {name}", peer.short());
                        AdminResponse::Devices {
                            devices: self.store.devices()?,
                        }
                    }
                    Some(_) => AdminResponse::Error {
                        message: "no such device".into(),
                    },
                    None => AdminResponse::Error {
                        message: "a device needs a name".into(),
                    },
                },
                AdminRequest::RemoveDevice { id } => {
                    if self.store.remove_device(&id)? {
                        info!(
                            "{} removed device {}",
                            peer.short(),
                            &id[..id.len().min(10)]
                        );
                    }
                    AdminResponse::Devices {
                        devices: self.store.devices()?,
                    }
                }
                AdminRequest::SetSettings { settings } => match self.settings.set(settings) {
                    Ok(()) => {
                        info!("{} changed the hub settings", peer.short());
                        AdminResponse::Settings { settings }
                    }
                    Err(e) => AdminResponse::Error {
                        message: format!("{e:#}"),
                    },
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Admin {
    async fn update(&self) -> Result<String> {
        //the guard must not live across an await
        let known = self.latest.borrow().clone();
        let version = match known {
            Some(version) => version,
            None => crate::update::newer_release(&self.http)
                .await?
                .ok_or_else(|| anyhow::anyhow!("this hub is up to date"))?,
        };
        crate::update::apply(&self.http, &version).await?;
        Ok(version)
    }
}

//windows installers restart the hub themselves
fn restart_soon() {
    #[cfg(not(windows))]
    tokio::spawn(async {
        //lets the answer reach the device
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if let Err(e) = crate::update::restart() {
            warn!("restart after update failed: {e:#}");
        }
    });
}

#[cfg(test)]
mod tests {
    use opera_hub_proto::{KIND_PHONE, SCOPE_SYNC};

    use super::*;

    fn store_with(kind: &str) -> Store {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).unwrap();
        let suffix: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let store = Store::new(&std::env::temp_dir().join(format!("opera-hub-admit-{suffix}")));
        let secret = store.start_pairing().unwrap();
        let device = Device {
            id: "known".into(),
            name: "device".into(),
            kind: kind.into(),
            scopes: default_scopes(kind),
            paired_at: 0,
            paired_by: None,
            last_seen: None,
            machine: None,
        };
        store.finish_pairing(&secret, device).unwrap().unwrap();
        store
    }

    #[test]
    fn denies_by_default() {
        let store = store_with("speaker");
        let unknown = Peer {
            id: "unknown".into(),
        };
        let known = Peer { id: "known".into() };
        assert!(admit(&store, &unknown, Access::Public).unwrap());
        assert!(!admit(&store, &unknown, Access::Paired).unwrap());
        assert!(!admit(&store, &unknown, Access::Scope(SCOPE_SYNC)).unwrap());
        assert!(admit(&store, &known, Access::Paired).unwrap());
        //unknown kinds get no scope
        assert!(!admit(&store, &known, Access::Scope(SCOPE_ADMIN)).unwrap());
    }

    #[test]
    fn phones_get_every_scope() {
        let store = store_with(KIND_PHONE);
        let known = Peer { id: "known".into() };
        assert!(admit(&store, &known, Access::Scope(SCOPE_ADMIN)).unwrap());
        assert!(store.device("known").unwrap().unwrap().last_seen.is_some());
    }
}
