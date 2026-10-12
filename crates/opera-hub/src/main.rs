mod config;
mod hub;
mod service;

use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};
use iroh_tickets::endpoint::EndpointTicket;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "opera-hub", version, about = "Opera Hub, the sync server for Opera devices")]
struct Cli {
    #[arg(long, global = true, help = "Hub data directory, defaults to the OS data directory")]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Run the hub in the foreground")]
    Run {
        #[arg(long, hide = true)]
        service: bool,
    },
    #[command(about = "Print the hub ticket and its QR code")]
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
        Command::Run { service } => {
            init_logging(&data_dir, service)?;
            #[cfg(windows)]
            if service {
                return service::windows::run(data_dir);
            }
            runtime()?.block_on(hub::run(&data_dir, shutdown_signal()))
        }
        Command::Ticket => {
            let ticket = hub::ticket(&data_dir)?.to_string();
            qr2term::print_qr(&ticket)?;
            println!("{ticket}");
            Ok(())
        }
        Command::Ping { ticket, count } => {
            init_logging(&data_dir, false)?;
            runtime()?.block_on(hub::ping(&data_dir, ticket, count))
        }
        Command::Service { action } => match action {
            ServiceAction::Install => service::install(&data_dir),
            ServiceAction::Uninstall => service::uninstall(),
            ServiceAction::Start => service::start(),
            ServiceAction::Stop => service::stop(),
            ServiceAction::Status => service::status(),
        },
    }
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Runtime::new()?)
}

//services have no console
fn init_logging(data_dir: &Path, to_file: bool) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,iroh=warn".into());
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if to_file {
        std::fs::create_dir_all(data_dir)?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(data_dir.join("hub.log"))?;
        builder.with_ansi(false).with_writer(std::sync::Mutex::new(file)).init();
    } else {
        builder.with_writer(std::io::stderr).init();
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        //systemd and launchd stop with sigterm
        use tokio::signal::unix::{signal, SignalKind};
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
