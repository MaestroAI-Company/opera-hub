//portable node and uv owned by the hub, nothing system wide
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use tokio::sync::Mutex;
use tracing::info;

use crate::llama::{extract, hex_sha256};

const NODE_VERSION: &str = "v24.22.0";
const NODE_DIST: &str = "https://nodejs.org/dist";
const UV_VERSION: &str = "0.13.1";
const UV_RELEASES: &str = "https://github.com/astral-sh/uv/releases/download";

pub struct Runtimes {
    dir: PathBuf,
    http: reqwest::Client,
    //two installs never race on one folder
    installing: Mutex<()>,
}

impl Runtimes {
    pub fn new(data_dir: &Path, http: reqwest::Client) -> Self {
        Self {
            dir: data_dir.join("runtimes"),
            http,
            installing: Mutex::new(()),
        }
    }

    fn node_root(&self) -> PathBuf {
        self.dir.join(format!("node-{NODE_VERSION}"))
    }

    fn uv_root(&self) -> PathBuf {
        self.dir.join(format!("uv-{UV_VERSION}"))
    }

    pub fn node_exe(&self) -> PathBuf {
        let root = self.node_root();
        if cfg!(windows) {
            root.join("node.exe")
        } else {
            root.join("bin").join("node")
        }
    }

    pub fn npm_cli(&self) -> PathBuf {
        let root = self.node_root();
        let modules = if cfg!(windows) {
            root.join("node_modules")
        } else {
            root.join("lib").join("node_modules")
        };
        modules.join("npm").join("bin").join("npm-cli.js")
    }

    pub fn uv_exe(&self) -> PathBuf {
        self.uv_root()
            .join(if cfg!(windows) { "uv.exe" } else { "uv" })
    }

    pub fn npm_cache(&self) -> PathBuf {
        self.dir.join("npm-cache")
    }

    //uv keeps python and its cache next to itself
    pub fn uv_env(&self) -> Vec<(OsString, OsString)> {
        vec![
            (
                "UV_PYTHON_INSTALL_DIR".into(),
                self.dir.join("python").into(),
            ),
            ("UV_CACHE_DIR".into(), self.dir.join("uv-cache").into()),
            ("UV_PYTHON_PREFERENCE".into(), "only-managed".into()),
            ("UV_NO_CONFIG".into(), "1".into()),
        ]
    }

    pub async fn ensure_node(&self) -> Result<()> {
        let _one = self.installing.lock().await;
        if self.node_exe().exists() {
            return Ok(());
        }
        let asset = format!("node-{NODE_VERSION}-{}", node_platform()?);
        let sums = self
            .text(&format!("{NODE_DIST}/{NODE_VERSION}/SHASUMS256.txt"))
            .await?;
        let expected = sums
            .lines()
            .find_map(|line| {
                let (hash, name) = line.split_once("  ")?;
                (name == asset).then(|| hash.to_string())
            })
            .with_context(|| format!("node publishes no checksum for {asset}"))?;
        info!("installing node {NODE_VERSION}");
        self.install(
            &format!("{NODE_DIST}/{NODE_VERSION}/{asset}"),
            &asset,
            &expected,
            &self.node_root(),
        )
        .await?;
        ensure!(self.node_exe().exists(), "the node archive has no node");
        Ok(())
    }

    pub async fn ensure_uv(&self) -> Result<()> {
        let _one = self.installing.lock().await;
        if self.uv_exe().exists() {
            return Ok(());
        }
        let asset = format!("uv-{}", uv_platform()?);
        let url = format!("{UV_RELEASES}/{UV_VERSION}/{asset}");
        let sum = self.text(&format!("{url}.sha256")).await?;
        let expected = sum
            .split_whitespace()
            .next()
            .context("empty uv checksum")?
            .to_string();
        info!("installing uv {UV_VERSION}");
        self.install(&url, &asset, &expected, &self.uv_root())
            .await?;
        ensure!(self.uv_exe().exists(), "the uv archive has no uv");
        Ok(())
    }

    async fn text(&self, url: &str) -> Result<String> {
        Ok(self
            .http
            .get(url)
            .header("user-agent", "opera-hub")
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?)
    }

    async fn install(&self, url: &str, asset: &str, expected: &str, target: &Path) -> Result<()> {
        let bytes = self
            .http
            .get(url)
            .header("user-agent", "opera-hub")
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        ensure!(
            hex_sha256(&bytes) == expected.to_ascii_lowercase(),
            "checksum mismatch for {asset}"
        );
        let staging = target.with_extension("partial");
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging)?;
        extract(asset, &bytes, &staging)?;
        //archives wrap everything in one folder
        let root = single_dir(&staging).unwrap_or(staging.clone());
        let _ = std::fs::remove_dir_all(target);
        std::fs::rename(&root, target)?;
        let _ = std::fs::remove_dir_all(&staging);
        Ok(())
    }
}

fn single_dir(dir: &Path) -> Option<PathBuf> {
    let mut entries = std::fs::read_dir(dir).ok()?.flatten();
    let first = entries.next()?;
    (entries.next().is_none() && first.path().is_dir()).then(|| first.path())
}

fn node_platform() -> Result<&'static str> {
    Ok(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => "win-x64.zip",
        ("macos", "aarch64") => "darwin-arm64.tar.gz",
        ("linux", "x86_64") => "linux-x64.tar.gz",
        ("linux", "aarch64") => "linux-arm64.tar.gz",
        (os, arch) => bail!("no node build for {os} {arch}"),
    })
}

fn uv_platform() -> Result<&'static str> {
    Ok(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => "x86_64-pc-windows-msvc.zip",
        ("macos", "aarch64") => "aarch64-apple-darwin.tar.gz",
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu.tar.gz",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu.tar.gz",
        (os, arch) => bail!("no uv build for {os} {arch}"),
    })
}

//a server only sees its runtime and its own folder
pub fn scrubbed_env(home: &Path, path: &[PathBuf]) -> Result<Vec<(OsString, OsString)>> {
    let mut dirs: Vec<PathBuf> = path.to_vec();
    let mut env: Vec<(OsString, OsString)> = Vec::new();
    let tmp = home.join("tmp");
    std::fs::create_dir_all(&tmp)?;
    if cfg!(windows) {
        //windows programs break without these
        for key in [
            "SystemRoot",
            "windir",
            "PATHEXT",
            "COMSPEC",
            "NUMBER_OF_PROCESSORS",
            "PROCESSOR_ARCHITECTURE",
        ] {
            if let Some(value) = std::env::var_os(key) {
                env.push((key.into(), value));
            }
        }
        if let Some(root) = std::env::var_os("SystemRoot") {
            dirs.push(PathBuf::from(root).join("System32"));
        }
        let appdata = home.join("AppData");
        let local = appdata.join("Local");
        std::fs::create_dir_all(&local)?;
        env.push(("USERPROFILE".into(), home.into()));
        env.push(("APPDATA".into(), appdata.join("Roaming").into()));
        env.push(("LOCALAPPDATA".into(), local.into()));
        env.push(("TEMP".into(), tmp.clone().into()));
        env.push(("TMP".into(), tmp.into()));
    } else {
        dirs.push("/usr/bin".into());
        dirs.push("/bin".into());
        env.push(("TMPDIR".into(), tmp.into()));
        env.push(("LANG".into(), "C.UTF-8".into()));
    }
    env.push(("HOME".into(), home.into()));
    env.push(("PATH".into(), std::env::join_paths(dirs)?));
    Ok(env)
}
