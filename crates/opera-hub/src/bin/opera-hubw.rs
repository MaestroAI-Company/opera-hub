//windowless hub started at login on windows
#![windows_subsystem = "windows"]

use std::path::PathBuf;

use anyhow::Result;
use opera_hub::{config, hub};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let data_dir = match (args.next().as_deref(), args.next()) {
        (Some("--data-dir"), Some(dir)) => PathBuf::from(dir),
        _ => config::default_data_dir()?,
    };
    hub::init_logging(&data_dir, true)?;
    let runtime = tokio::runtime::Runtime::new()?;
    //stops with the session or taskkill
    let result = runtime.block_on(hub::run(&data_dir, std::future::pending()));
    if let Err(e) = &result {
        tracing::error!("hub stopped: {e:#}");
    }
    result
}
