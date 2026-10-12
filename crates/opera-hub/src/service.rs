pub use imp::{install, start, status, stop, uninstall};

//systemd user unit or launchd agent
#[cfg(not(windows))]
mod imp {
    use std::ffi::OsString;
    use std::path::Path;

    use anyhow::{Context, Result};
    use service_manager::{
        RestartPolicy, ServiceInstallCtx, ServiceLabel, ServiceLevel, ServiceManager,
        ServiceStartCtx, ServiceStatus, ServiceStatusCtx, ServiceStopCtx, ServiceUninstallCtx,
    };

    const LABEL: &str = "company.maestroai.opera-hub";

    fn label() -> ServiceLabel {
        LABEL.parse().expect("static service label")
    }

    fn manager() -> Result<Box<dyn ServiceManager>> {
        let mut manager = <dyn ServiceManager>::native().context("no supported service manager")?;
        manager.set_level(ServiceLevel::User)?;
        Ok(manager)
    }

    pub fn install(data_dir: &Path) -> Result<()> {
        let args: Vec<OsString> = vec!["--data-dir".into(), data_dir.into(), "run".into()];
        let manager = manager()?;
        manager.install(ServiceInstallCtx {
            label: label(),
            program: std::env::current_exe()?,
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
        })?;
        manager.start(ServiceStartCtx { label: label() })?;
        println!("service installed and started");
        if cfg!(target_os = "linux") {
            println!(
                "to start it at boot without a login session, run: loginctl enable-linger $USER"
            );
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

    pub fn start(_data_dir: &Path) -> Result<()> {
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
}

//login start under the user so the keychain matches
#[cfg(windows)]
mod imp {
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    use anyhow::{Result, bail, ensure};

    const RUN_KEY: &str = r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run";
    const RUN_VALUE: &str = "OperaHub";
    const WINDOWLESS: &str = "opera-hubw.exe";

    fn windowless() -> Result<PathBuf> {
        let path = std::env::current_exe()?.with_file_name(WINDOWLESS);
        ensure!(
            path.exists(),
            "{WINDOWLESS} is missing next to opera-hub.exe"
        );
        Ok(path)
    }

    fn quiet(program: &str, args: &[&str]) -> Result<bool> {
        let status = Command::new(program)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        Ok(status.success())
    }

    fn running() -> Result<bool> {
        let filter = format!("IMAGENAME eq {WINDOWLESS}");
        let output = Command::new("tasklist")
            .args(["/FI", &filter, "/NH"])
            .output()?;
        Ok(String::from_utf8_lossy(&output.stdout).contains(WINDOWLESS))
    }

    pub fn install(data_dir: &Path) -> Result<()> {
        let command = format!(
            "\"{}\" --data-dir \"{}\"",
            windowless()?.display(),
            data_dir.display()
        );
        let args = [
            "add", RUN_KEY, "/v", RUN_VALUE, "/t", "REG_SZ", "/d", &command, "/f",
        ];
        if !quiet("reg", &args)? {
            bail!("cannot write the login entry");
        }
        start(data_dir)?;
        println!("installed, the hub starts at login and runs now");
        Ok(())
    }

    pub fn uninstall() -> Result<()> {
        stop()?;
        quiet("reg", &["delete", RUN_KEY, "/v", RUN_VALUE, "/f"])?;
        println!("uninstalled");
        Ok(())
    }

    pub fn start(data_dir: &Path) -> Result<()> {
        if !running()? {
            Command::new(windowless()?)
                .arg("--data-dir")
                .arg(data_dir)
                .spawn()?;
        }
        Ok(())
    }

    pub fn stop() -> Result<()> {
        if running()? {
            quiet("taskkill", &["/IM", WINDOWLESS, "/F"])?;
        }
        Ok(())
    }

    pub fn status() -> Result<()> {
        let installed = quiet("reg", &["query", RUN_KEY, "/v", RUN_VALUE])?;
        let state = match (installed, running()?) {
            (false, _) => "not installed",
            (true, true) => "running",
            (true, false) => "stopped",
        };
        println!("{state}");
        Ok(())
    }
}
