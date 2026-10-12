use std::future::Future;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use iroh::endpoint::{presets, Connection};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, SecretKey};
use iroh_tickets::endpoint::EndpointTicket;
use tracing::info;

use crate::config::{self, Config};

pub const PING_ALPN: &[u8] = b"opera/ping/0";

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
    let config = Config::load(data_dir)?;
    let key = config::load_or_create_key(data_dir)?;
    let endpoint = bind(&config, Some(key), config.port).await?;
    info!("hub {} started, data in {}", endpoint.id(), data_dir.display());

    //relays reconnect if offline at boot
    tokio::spawn({
        let endpoint = endpoint.clone();
        async move {
            endpoint.online().await;
            info!("hub reachable");
        }
    });

    let router = Router::builder(endpoint).accept(PING_ALPN, Ping).spawn();
    shutdown.await;
    info!("hub stopping");
    router.shutdown().await?;
    Ok(())
}

//clients find addresses through lookup
pub fn ticket(data_dir: &Path) -> Result<EndpointTicket> {
    let config = Config::load(data_dir)?;
    let key = config::load_or_create_key(data_dir)?;
    let addr = config
        .relays
        .iter()
        .fold(EndpointAddr::new(key.public()), |addr, url| addr.with_relay_url(url.clone()));
    Ok(EndpointTicket::new(addr))
}

pub async fn ping(data_dir: &Path, ticket: EndpointTicket, count: u32) -> Result<()> {
    let config = Config::load(data_dir)?;
    let endpoint = bind(&config, None, None).await?;
    let started = Instant::now();
    let conn = endpoint.connect(ticket.endpoint_addr().clone(), PING_ALPN).await?;
    println!("connected in {:?}", started.elapsed());

    for i in 1..=count {
        let started = Instant::now();
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(b"ping").await?;
        send.finish()?;
        let echo = recv.read_to_end(16).await?;
        ensure!(echo == b"ping", "hub answered with unexpected bytes");
        println!("{i}: {:?} via {}", started.elapsed(), path_kind(&conn));
        if i < count {
            //leaves time for hole punching
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

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

//reachability check
#[derive(Debug, Clone)]
struct Ping;

impl ProtocolHandler for Ping {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        info!("ping from {}", conn.remote_id().fmt_short());
        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
            tokio::io::copy(&mut recv, &mut send).await?;
            send.finish()?;
        }
        Ok(())
    }
}
