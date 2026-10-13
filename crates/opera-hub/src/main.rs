mod service;

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use iroh_tickets::endpoint::EndpointTicket;
use opera_hub::store::{self, Store};
use opera_hub::{config, hub, invite};

#[derive(Parser)]
#[command(
    name = "opera-hub",
    version,
    about = "Opera Hub, the sync server for Opera devices"
)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Hub data directory, defaults to the OS data directory"
    )]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Run the hub in the foreground")]
    Run,
    #[command(about = "Show a one-time QR code and code to pair a device")]
    Pair,
    #[command(about = "List or remove paired devices")]
    Devices {
        #[command(subcommand)]
        action: Option<DevicesAction>,
    },
    #[command(about = "Print the hub ticket and its QR code, for ping")]
    Ticket,
    #[command(about = "Check that a hub is reachable")]
    Ping {
        ticket: EndpointTicket,
        #[arg(short, long, default_value_t = 5)]
        count: u32,
    },
    #[command(about = "Manage the hub background service")]
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
}

#[derive(Subcommand)]
enum DevicesAction {
    List,
    #[command(about = "Unpair a device by id or id prefix")]
    Remove {
        id: String,
    },
}

#[derive(Subcommand)]
enum ServiceAction {
    Install,
    Uninstall,
    Start,
    Stop,
    Status,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = match cli.data_dir {
        Some(dir) => dir,
        None => config::default_data_dir()?,
    };

    match cli.command {
        Command::Run => {
            hub::init_logging(&data_dir, false)?;
            runtime()?.block_on(hub::run(&data_dir, shutdown_signal()))
        }
        Command::Pair => {
            hub::init_logging(&data_dir, false)?;
            runtime()?.block_on(async {
                let invite = invite::open(&data_dir).await?;
                let link = invite.link.to_string();
                qr2term::print_qr(&link)?;
                println!("{link}");
                println!("code: {}", invite.code);
                println!(
                    "scan the QR code or type the code in Opera within {} minutes, the hub must be running",
                    store::PAIRING_TTL.as_secs() / 60
                );
                //the code is answered from here
                tokio::select! {
                    used = invite.done => {
                        if used.unwrap_or(false) {
                            println!("code used, the device finishes pairing");
                        } else {
                            println!("the code expired");
                        }
                    }
                    _ = shutdown_signal() => {}
                }
                Ok(())
            })
        }
        Command::Devices { action } => devices(&data_dir, action.unwrap_or(DevicesAction::List)),
        Command::Ticket => {
            let ticket = hub::ticket(&data_dir)?.to_string();
            qr2term::print_qr(&ticket)?;
            println!("{ticket}");
            Ok(())
        }
        Command::Ping { ticket, count } => {
            hub::init_logging(&data_dir, false)?;
            runtime()?.block_on(hub::ping(&data_dir, ticket, count))
        }
        Command::Service { action } => match action {
            ServiceAction::Install => service::install(&data_dir),
            ServiceAction::Uninstall => service::uninstall(),
            ServiceAction::Start => service::start(&data_dir),
            ServiceAction::Stop => service::stop(),
            ServiceAction::Status => service::status(),
        },
    }
}

fn devices(data_dir: &Path, action: DevicesAction) -> Result<()> {
    let store = Store::new(data_dir);
    match action {
        DevicesAction::List => {
            let devices = store.devices()?;
            if devices.is_empty() {
                println!("no paired device, run: opera-hub pair");
            }
            for device in devices {
                println!(
                    "{}  {} ({}) {}",
                    device.id,
                    device.name,
                    device.kind,
                    device.scopes.join(",")
                );
            }
        }
        DevicesAction::Remove { id } => {
            let matches: Vec<_> = store
                .devices()?
                .into_iter()
                .filter(|d| d.id.starts_with(&id))
                .collect();
            match matches.as_slice() {
                [device] => {
                    store.remove_device(&device.id)?;
                    println!("removed {}", device.name);
                }
                [] => bail!("no paired device matches {id}"),
                _ => bail!("{id} matches several devices, use a longer prefix"),
            }
        }
    }
    Ok(())
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Runtime::new()?)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        //systemd and launchd stop with sigterm
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("sigterm handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
