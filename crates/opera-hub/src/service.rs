use std::ffi::OsString;
use std::path::Path;

use anyhow::{Context, Result};
use service_manager::{
    RestartPolicy, ServiceInstallCtx, ServiceLabel, ServiceLevel, ServiceManager, ServiceStartCtx,
    ServiceStatus, ServiceStatusCtx, ServiceStopCtx, ServiceUninstallCtx,
};

const LABEL: &str = "company.maestroai.opera-hub";

fn label() -> ServiceLabel {
    LABEL.parse().expect("static service label")
}

fn manager() -> Result<Box<dyn ServiceManager>> {
    let mut manager = <dyn ServiceManager>::native().context("no supported service manager")?;
    //windows services are system wide only
    if !cfg!(windows) {
        manager.set_level(ServiceLevel::User)?;
    }
    Ok(manager)
}

pub fn install(data_dir: &Path) -> Result<()> {
    let program = std::env::current_exe()?;
    //service may run as another user
    let mut args: Vec<OsString> = vec!["--data-dir".into(), data_dir.into(), "run".into()];
    if cfg!(windows) {
        args.push("--service".into());
    }
    let manager = manager()?;
    manager
        .install(ServiceInstallCtx {
            label: label(),
            program,
            args,
            contents: None,
            username: None,
            working_directory: None,
            environment: None,
            autostart: true,
            restart_policy: RestartPolicy::OnFailure {
                delay_secs: Some(10),
                max_retries: None,
                reset_after_secs: None,
            },
        })
        .context("service install failed, on windows run it as administrator")?;
    //sc create ignores restart policies
    #[cfg(windows)]
    std::process::Command::new("sc.exe")
        .args([
            "failure",
            LABEL,
            "reset=",
            "86400",
            "actions=",
            "restart/10000",
        ])
        .status()?;
    manager.start(ServiceStartCtx { label: label() })?;
    println!("service installed and started");
    if cfg!(target_os = "linux") {
        println!("to start it at boot without a login session, run: loginctl enable-linger $USER");
    }
    Ok(())
}

pub fn uninstall() -> Result<()> {
    let manager = manager()?;
    //already stopped is fine
    let _ = manager.stop(ServiceStopCtx { label: label() });
    manager.uninstall(ServiceUninstallCtx { label: label() })?;
    println!("service uninstalled");
    Ok(())
}

pub fn start() -> Result<()> {
    manager()?.start(ServiceStartCtx { label: label() })?;
    Ok(())
}

pub fn stop() -> Result<()> {
    manager()?.stop(ServiceStopCtx { label: label() })?;
    Ok(())
}

pub fn status() -> Result<()> {
    match manager()?.status(ServiceStatusCtx { label: label() })? {
        ServiceStatus::NotInstalled => println!("not installed"),
        ServiceStatus::Running => println!("running"),
        ServiceStatus::Stopped(Some(reason)) => println!("stopped ({reason})"),
        ServiceStatus::Stopped(None) => println!("stopped"),
    }
    Ok(())
}

//scm needs the binary to report its state itself
#[cfg(windows)]
pub mod windows {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use anyhow::Result;
    use tracing::error;
    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::{define_windows_service, service_dispatcher};

    use super::LABEL;

    static DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

    define_windows_service!(ffi_service_main, service_main);

    pub fn run(data_dir: PathBuf) -> Result<()> {
        let _ = DATA_DIR.set(data_dir);
        service_dispatcher::start(LABEL, ffi_service_main)?;
        Ok(())
    }

    fn service_main(_args: Vec<OsString>) {
        if let Err(e) = run_service() {
            error!("service failed: {e:#}");
        }
    }

    fn run_service() -> Result<()> {
        let data_dir = DATA_DIR.get().expect("data dir set before dispatch");
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let stop_tx = Mutex::new(Some(stop_tx));
        let handle = service_control_handler::register(LABEL, move |control| match control {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                if let Some(tx) = stop_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        })?;

        handle.set_service_status(status(
            ServiceState::Running,
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            0,
        ))?;
        let runtime = tokio::runtime::Runtime::new()?;
        let result = runtime.block_on(opera_hub::hub::run(data_dir, async {
            let _ = stop_rx.await;
        }));
        //non zero exit triggers scm restart
        let code = if result.is_ok() { 0 } else { 1 };
        handle.set_service_status(status(
            ServiceState::Stopped,
            ServiceControlAccept::empty(),
            code,
        ))?;
        result
    }

    fn status(state: ServiceState, accept: ServiceControlAccept, code: u32) -> ServiceStatus {
        ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: state,
            controls_accepted: accept,
            exit_code: ServiceExitCode::Win32(code),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        }
    }
}
