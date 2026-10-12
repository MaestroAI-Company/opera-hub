use std::future::Future;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use iroh::endpoint::{Connection, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, SecretKey};
use iroh_tickets::endpoint::EndpointTicket;
use opera_hub_proto::{
    ADMIN_ALPN, HUB_ALPN, PAIR_ALPN, PING_ALPN, PROVIDERS_ALPN, PairingLink, SECRETS_ALPN,
    SYNC_ALPN, UNAUTHORIZED, hub_addr,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{self, Config};
use crate::db::Db;
use crate::protocols::{Admin, Pair, Peer, Ping, Protocol, Session, admit};
use crate::providers::{Providers, Target, default_targets};
use crate::secrets::{Secrets, http_client};
use crate::store::Store;
use crate::sync::DataSync;

async fn bind(config: &Config, key: Option<SecretKey>, port: Option<u16>) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::N0).relay_mode(config.relay_mode());
    if let Some(key) = key {
        builder = builder.secret_key(key);
    }
    if let Some(port) = port {
        builder = builder
            .bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))?
            .bind_addr(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)))?;
    }
    Ok(builder.bind().await?)
}

pub async fn run(data_dir: &Path, shutdown: impl Future<Output = ()>) -> Result<()> {
    run_with_targets(data_dir, shutdown, default_targets()).await
}

//tests point the relay at fake providers
pub async fn run_with_targets(
    data_dir: &Path,
    shutdown: impl Future<Output = ()>,
    targets: Vec<Target>,
) -> Result<()> {
    let config = Config::load(data_dir)?;
    let key = config::load_or_create_key(data_dir)?;
    let endpoint = bind(&config, Some(key), config.port).await?;
    info!(
        "hub {} started, data in {}",
        endpoint.id(),
        data_dir.display()
    );

    //relays reconnect if offline at boot
    tokio::spawn({
        let endpoint = endpoint.clone();
        async move {
            endpoint.online().await;
            info!("hub reachable");
        }
    });

    let store = Arc::new(Store::new(data_dir));
    let db = Arc::new(Db::open(data_dir)?);
    let http = http_client()?;
    let name: Arc<str> = config.hub_name().into();
    let router = Router::builder(endpoint)
        .accept(PING_ALPN, OverIroh::new(Ping, &store))
        .accept(
            PAIR_ALPN,
            OverIroh::new(
                Pair {
                    store: store.clone(),
                    name: name.clone(),
                },
                &store,
            ),
        )
        .accept(
            HUB_ALPN,
            OverIroh::new(
                Session {
                    store: store.clone(),
                    name,
                },
                &store,
            ),
        )
        .accept(
            ADMIN_ALPN,
            OverIroh::new(
                Admin {
                    store: store.clone(),
                },
                &store,
            ),
        )
        .accept(
            SYNC_ALPN,
            OverIroh::new(DataSync { db: db.clone() }, &store),
        )
        .accept(
            SECRETS_ALPN,
            OverIroh::new(
                Secrets {
                    db,
                    http: http.clone(),
                    refreshing: Default::default(),
                },
                &store,
            ),
        )
        .accept(
            PROVIDERS_ALPN,
            OverIroh::new(
                Providers {
                    http,
                    targets: Arc::new(targets),
                },
                &store,
            ),
        )
        .spawn();
    shutdown.await;
    info!("hub stopping");
    router.shutdown().await?;
    Ok(())
}

//clients find addresses through lookup
pub fn ticket(data_dir: &Path) -> Result<EndpointTicket> {
    let config = Config::load(data_dir)?;
    let key = config::load_or_create_key(data_dir)?;
    Ok(EndpointTicket::new(hub_addr(key.public(), &config.relays)))
}

pub fn pairing_link(data_dir: &Path) -> Result<PairingLink> {
    let config = Config::load(data_dir)?;
    let key = config::load_or_create_key(data_dir)?;
    let secret = Store::new(data_dir).start_pairing()?;
    Ok(PairingLink {
        hub: key.public(),
        relays: config.relays,
        secret,
    })
}

pub async fn ping(data_dir: &Path, ticket: EndpointTicket, count: u32) -> Result<()> {
    let config = Config::load(data_dir)?;
    let endpoint = bind(&config, None, None).await?;
    let started = Instant::now();
    let conn = endpoint
        .connect(ticket.endpoint_addr().clone(), PING_ALPN)
        .await?;
    println!("connected in {:?}", started.elapsed());

    let (mut send, mut recv) = conn.open_bi().await?;
    for i in 1..=count {
        let started = Instant::now();
        send.write_all(b"ping").await?;
        let mut echo = [0u8; 4];
        recv.read_exact(&mut echo).await?;
        ensure!(&echo == b"ping", "hub answered with unexpected bytes");
        println!("{i}: {:?} via {}", started.elapsed(), path_kind(&conn));
        if i < count {
            //leaves time for hole punching
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    send.finish()?;
    conn.close(0u32.into(), b"bye");
    endpoint.close().await;
    Ok(())
}

fn path_kind(conn: &Connection) -> &'static str {
    match conn.paths().iter().find(|path| path.is_selected()) {
        Some(path) if path.is_ip() => "direct",
        Some(path) if path.is_relay() => "relay",
        _ => "unknown",
    }
}

fn user_error(e: anyhow::Error) -> AcceptError {
    AcceptError::from_boxed(e.into())
}

//each bi stream is one protocol session
#[derive(Debug, Clone)]
struct OverIroh<P> {
    protocol: P,
    store: Arc<Store>,
}

impl<P> OverIroh<P> {
    fn new(protocol: P, store: &Arc<Store>) -> Self {
        Self {
            protocol,
            store: store.clone(),
        }
    }
}

impl<P: Protocol + std::fmt::Debug> ProtocolHandler for OverIroh<P> {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let peer = Peer {
            id: conn.remote_id().to_string(),
        };
        if !admit(&self.store, &peer, P::ACCESS).map_err(user_error)? {
            conn.close(UNAUTHORIZED.into(), b"unauthorized");
            return Ok(());
        }
        //ends when the client closes the connection
        while let Ok((mut send, recv)) = conn.accept_bi().await {
            let protocol = self.protocol.clone();
            let peer = peer.clone();
            tokio::spawn(async move {
                match protocol.serve(peer, recv, &mut send).await {
                    Ok(()) => {
                        let _ = send.finish();
                    }
                    Err(e) => warn!("stream failed: {e:#}"),
                }
            });
        }
        Ok(())
    }
}

//windowless runs have no console
pub fn init_logging(data_dir: &Path, to_file: bool) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,iroh=warn".into());
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if to_file {
        std::fs::create_dir_all(data_dir)?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(data_dir.join("hub.log"))?;
        builder
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(file))
            .init();
    } else {
        builder.with_writer(std::io::stderr).init();
    }
    Ok(())
}
