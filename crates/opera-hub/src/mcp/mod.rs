//mcp servers the hub installs, runs and shares
mod bridge;
mod mcpb;
mod runtime;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use opera_hub_proto::{ForwardHead, ProviderInfo, ProvidersResponse, write_frame};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub use bridge::Bridge;
use mcpb::Manifest;
use runtime::{Runtimes, scrubbed_env};

use crate::llama::hex_sha256;

//providers ids of hosted servers
pub const PROVIDER_PREFIX: &str = "mcp-";
const SPEC_FILE: &str = "server.json";
const LOG_FILE: &str = "server.log";
const MAX_LOG_BYTES: u64 = 1024 * 1024;
const TAIL_LINES: usize = 500;
const MAX_BODY_BYTES: u64 = 4 * 1024 * 1024;
//a server crashing this often stays down
const MAX_CRASHES: usize = 5;
const CRASH_WINDOW: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Source {
    Mcpb { url: String, sha256: String },
    Npm { package: String, version: String },
    Pypi { package: String, version: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Spec {
    pub name: String,
    pub source: Source,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub config: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum State {
    Starting,
    Running,
    Failed(String),
}

struct Server {
    id: String,
    dir: PathBuf,
    spec: Spec,
    state: Mutex<State>,
    bridge: Mutex<Option<Arc<Bridge>>>,
    tail: Mutex<VecDeque<String>>,
}

//a server and the task keeping it alive
type Supervised = (Arc<Server>, JoinHandle<()>);

pub struct Mcp {
    dir: PathBuf,
    http: reqwest::Client,
    runtimes: Arc<Runtimes>,
    servers: Mutex<HashMap<String, Supervised>>,
}

impl std::fmt::Debug for Mcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Mcp")
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        self.stop_all();
    }
}

impl Mcp {
    pub fn new(data_dir: &Path, http: reqwest::Client) -> Arc<Self> {
        Arc::new(Self {
            dir: data_dir.join("mcp"),
            runtimes: Arc::new(Runtimes::new(data_dir, http.clone())),
            http,
            servers: Mutex::new(HashMap::new()),
        })
    }

    //servers installed earlier start with the hub
    pub fn start_installed(&self) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let id = entry.file_name().to_string_lossy().into_owned();
            match read_spec(&entry.path()) {
                Ok(spec) => self.spawn(id, spec),
                Err(e) => warn!("mcp server {id} skipped: {e:#}"),
            }
        }
    }

    pub fn stop_all(&self) {
        for (_, (_, task)) in self.servers.lock().unwrap().drain() {
            //kill on drop ends the child
            task.abort();
        }
    }

    pub async fn install(&self, spec: Spec) -> Result<String> {
        check_source(&spec.source)?;
        let id = new_id()?;
        let dir = self.dir.join(&id);
        let result = self.install_into(&dir, &spec).await;
        if let Err(e) = result {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(e);
        }
        std::fs::write(dir.join(SPEC_FILE), serde_json::to_vec_pretty(&spec)?)?;
        info!("mcp server {} installed as {id}", spec.name);
        self.spawn(id.clone(), spec);
        Ok(id)
    }

    pub async fn remove(&self, id: &str) -> Result<()> {
        let entry = self.servers.lock().unwrap().remove(id);
        let (server, task) = entry.ok_or_else(|| anyhow!("no mcp server {id}"))?;
        task.abort();
        //windows keeps files locked until the child exits
        for _ in 0..20 {
            if std::fs::remove_dir_all(&server.dir).is_ok() || !server.dir.exists() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        std::fs::remove_dir_all(&server.dir)
            .with_context(|| format!("cannot delete {}", server.dir.display()))
    }

    pub fn state(&self, id: &str) -> Option<State> {
        let servers = self.servers.lock().unwrap();
        let (server, _) = servers.get(id)?;
        Some(server.state.lock().unwrap().clone())
    }

    pub fn log_tail(&self, id: &str) -> Vec<String> {
        let servers = self.servers.lock().unwrap();
        servers
            .get(id)
            .map(|(s, _)| s.tail.lock().unwrap().iter().cloned().collect())
            .unwrap_or_default()
    }

    //running servers devices can call
    pub fn providers(&self) -> Vec<ProviderInfo> {
        let servers = self.servers.lock().unwrap();
        let mut list: Vec<ProviderInfo> = servers
            .values()
            .filter(|(s, _)| *s.state.lock().unwrap() == State::Running)
            .map(|(s, _)| ProviderInfo {
                id: format!("{PROVIDER_PREFIX}{}", s.id),
                name: s.spec.name.clone(),
                kind: "mcp".into(),
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    }

    //plain json answers, no session or stream
    pub async fn forward<R, W>(
        &self,
        id: &str,
        head: &ForwardHead,
        reader: R,
        writer: &mut W,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let reply = |status: u16, body: Option<Value>| (status, body);
        let (status, body) = if head.method.eq_ignore_ascii_case("DELETE") {
            reply(200, None)
        } else if !head.method.eq_ignore_ascii_case("POST") {
            reply(405, None)
        } else {
            let mut raw = Vec::new();
            reader.take(MAX_BODY_BYTES).read_to_end(&mut raw).await?;
            let bridge = {
                let servers = self.servers.lock().unwrap();
                servers
                    .get(id)
                    .and_then(|(s, _)| s.bridge.lock().unwrap().clone())
            };
            match (serde_json::from_slice::<Value>(&raw), bridge) {
                (Err(_), _) => reply(400, Some(rpc_error(Value::Null, -32700, "invalid json"))),
                (Ok(message), None) => reply(
                    503,
                    Some(rpc_error(
                        message.get("id").cloned().unwrap_or(Value::Null),
                        -32603,
                        "this mcp server is not running",
                    )),
                ),
                (Ok(message), Some(bridge)) => match bridge.handle(message).await {
                    Some(answer) => reply(200, Some(answer)),
                    None => reply(202, None),
                },
            }
        };
        let headers = if body.is_some() {
            vec![("content-type".to_string(), "application/json".to_string())]
        } else {
            Vec::new()
        };
        write_frame(writer, &ProvidersResponse::Response { status, headers }).await?;
        if let Some(body) = body {
            writer.write_all(body.to_string().as_bytes()).await?;
        }
        writer.flush().await?;
        Ok(())
    }

    fn spawn(&self, id: String, spec: Spec) {
        let dir = self.dir.join(&id);
        let server = Arc::new(Server {
            id: id.clone(),
            dir,
            spec,
            state: Mutex::new(State::Starting),
            bridge: Mutex::new(None),
            tail: Mutex::new(VecDeque::new()),
        });
        let task = tokio::spawn(supervise(self.runtimes.clone(), server.clone()));
        self.servers.lock().unwrap().insert(id, (server, task));
    }

    async fn install_into(&self, dir: &Path, spec: &Spec) -> Result<()> {
        let package = dir.join("package");
        let home = dir.join("home");
        std::fs::create_dir_all(&package)?;
        std::fs::create_dir_all(&home)?;
        match &spec.source {
            Source::Mcpb { url, sha256 } => {
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
                    hex_sha256(&bytes) == sha256.to_ascii_lowercase(),
                    "checksum mismatch for the bundle"
                );
                mcpb::unpack(&bytes, &package)?;
                let manifest = Manifest::read(&package)?;
                manifest.check_supported()?;
                //fails early on a missing value
                manifest.launch(&package, &home, &spec.config)?;
            }
            Source::Npm {
                package: name,
                version,
            } => {
                self.runtimes.ensure_node().await?;
                let mut command = Command::new(self.runtimes.node_exe());
                command
                    .arg(self.runtimes.npm_cli())
                    .args(["install", "--prefix"])
                    .arg(&package)
                    .args(["--ignore-scripts", "--no-audit", "--no-fund", "--omit=dev"])
                    .args(["--loglevel=error", &format!("{name}@{version}")])
                    .env("npm_config_cache", self.runtimes.npm_cache())
                    .env("npm_config_update_notifier", "false");
                run_tool(command, &home, &self.node_path()).await?;
                npm_entry(&package, name)?;
            }
            Source::Pypi {
                package: name,
                version,
            } => {
                self.runtimes.ensure_uv().await?;
                let venv = package.join("venv");
                let mut command = Command::new(self.runtimes.uv_exe());
                command.arg("venv").arg(&venv).envs(self.runtimes.uv_env());
                run_tool(command, &home, &[]).await?;
                let mut command = Command::new(self.runtimes.uv_exe());
                command
                    .args(["pip", "install", &format!("{name}=={version}")])
                    .env("VIRTUAL_ENV", &venv)
                    .envs(self.runtimes.uv_env());
                run_tool(command, &home, &[]).await?;
                python_entry(&venv, name)?;
            }
        }
        Ok(())
    }

    fn node_path(&self) -> Vec<PathBuf> {
        self.runtimes
            .node_exe()
            .parent()
            .map(Path::to_path_buf)
            .into_iter()
            .collect()
    }
}

fn read_spec(dir: &Path) -> Result<Spec> {
    Ok(serde_json::from_slice(&std::fs::read(
        dir.join(SPEC_FILE),
    )?)?)
}

fn new_id() -> Result<String> {
    let mut bytes = [0u8; 6];
    getrandom::fill(&mut bytes).map_err(|e| anyhow!("no system randomness: {e}"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

//restarts crashed servers until they crash too often
async fn supervise(runtimes: Arc<Runtimes>, server: Arc<Server>) {
    let mut crashes: VecDeque<Instant> = VecDeque::new();
    loop {
        *server.state.lock().unwrap() = State::Starting;
        let error = match run_once(&runtimes, &server).await {
            Ok(status) => format!("exited with {status}"),
            Err(e) => format!("{e:#}"),
        };
        *server.bridge.lock().unwrap() = None;
        warn!("mcp server {} stopped: {error}", server.spec.name);
        let now = Instant::now();
        crashes.push_back(now);
        crashes.retain(|t| now.duration_since(*t) < CRASH_WINDOW);
        if crashes.len() >= MAX_CRASHES {
            *server.state.lock().unwrap() = State::Failed(error);
            return;
        }
        *server.state.lock().unwrap() = State::Failed(error);
        let backoff = Duration::from_secs(1 << crashes.len().min(5));
        tokio::time::sleep(backoff).await;
    }
}

async fn run_once(runtimes: &Runtimes, server: &Arc<Server>) -> Result<std::process::ExitStatus> {
    let package = server.dir.join("package");
    let home = server.dir.join("home");
    let (program, args, extra, path) = launch(runtimes, &server.spec, &package, &home).await?;
    let mut env = scrubbed_env(&home, &path)?;
    env.extend(extra);
    let mut command = Command::new(program);
    command
        .args(args)
        .env_clear()
        .envs(env)
        .current_dir(&home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    let mut child = command.spawn().context("cannot start the server")?;

    let stderr = child.stderr.take().context("no stderr")?;
    tokio::spawn(keep_log(stderr, server.clone()));
    let stdout = child.stdout.take().context("no stdout")?;
    let stdin = child.stdin.take().context("no stdin")?;
    let bridge = Bridge::connect(stdout, stdin, Vec::new()).await?;
    *server.bridge.lock().unwrap() = Some(bridge);
    *server.state.lock().unwrap() = State::Running;
    info!("mcp server {} running", server.spec.name);
    Ok(child.wait().await?)
}

//program, args, env and path for one start
async fn launch(
    runtimes: &Runtimes,
    spec: &Spec,
    package: &Path,
    home: &Path,
) -> Result<(
    PathBuf,
    Vec<String>,
    Vec<(OsString, OsString)>,
    Vec<PathBuf>,
)> {
    match &spec.source {
        Source::Npm { package: name, .. } => {
            runtimes.ensure_node().await?;
            let node = runtimes.node_exe();
            let mut args = vec![npm_entry(package, name)?.to_string_lossy().into_owned()];
            args.extend(spec.args.iter().cloned());
            let path = node.parent().map(Path::to_path_buf).into_iter().collect();
            Ok((node, args, Vec::new(), path))
        }
        Source::Pypi { package: name, .. } => {
            let venv = package.join("venv");
            let script = python_entry(&venv, name)?;
            let path = script.parent().map(Path::to_path_buf).into_iter().collect();
            Ok((script, spec.args.clone(), Vec::new(), path))
        }
        Source::Mcpb { .. } => {
            let manifest = Manifest::read(package)?;
            let launch = manifest.launch(package, home, &spec.config)?;
            let program = match launch.command.as_str() {
                "node" => {
                    runtimes.ensure_node().await?;
                    runtimes.node_exe()
                }
                "python" | "python3" => managed_python(runtimes, home).await?,
                other => inside(package, Path::new(other))?,
            };
            let env = launch
                .env
                .into_iter()
                .map(|(k, v)| (OsString::from(k), OsString::from(v)))
                .collect();
            let path = program
                .parent()
                .map(Path::to_path_buf)
                .into_iter()
                .collect();
            Ok((program, launch.args, env, path))
        }
    }
}

async fn managed_python(runtimes: &Runtimes, home: &Path) -> Result<PathBuf> {
    runtimes.ensure_uv().await?;
    let mut install = Command::new(runtimes.uv_exe());
    install.args(["python", "install"]).envs(runtimes.uv_env());
    run_tool(install, home, &[]).await?;
    let found = Command::new(runtimes.uv_exe())
        .args(["python", "find"])
        .envs(runtimes.uv_env())
        .output()
        .await?;
    ensure!(found.status.success(), "no managed python found");
    Ok(PathBuf::from(String::from_utf8(found.stdout)?.trim()))
}

//bundled programs never leave the package
fn inside(package: &Path, program: &Path) -> Result<PathBuf> {
    let full = if program.is_absolute() {
        program.to_path_buf()
    } else {
        package.join(program)
    };
    let real = full
        .canonicalize()
        .with_context(|| format!("the bundle has no {}", program.display()))?;
    ensure!(
        real.starts_with(package.canonicalize()?),
        "the bundle runs a program outside itself"
    );
    Ok(real)
}

//the command an npm package declares
fn npm_entry(package: &Path, name: &str) -> Result<PathBuf> {
    let root = package.join("node_modules").join(name);
    let manifest: Value = serde_json::from_slice(&std::fs::read(root.join("package.json"))?)?;
    let short = name.rsplit('/').next().unwrap_or(name);
    let bin = match &manifest["bin"] {
        Value::String(bin) => bin.clone(),
        Value::Object(bins) => bins
            .get(short)
            .or_else(|| bins.values().next())
            .and_then(Value::as_str)
            .context("invalid bin entry")?
            .to_string(),
        _ => bail!("{name} declares no command"),
    };
    let bin = Path::new(&bin);
    ensure!(
        bin.components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir)),
        "{name} points its command outside the package"
    );
    Ok(root.join(bin))
}

//the console script a python package declares
fn python_entry(venv: &Path, name: &str) -> Result<PathBuf> {
    let normalize = |s: &str| s.to_ascii_lowercase().replace(['-', '.'], "_");
    let wanted = normalize(name);
    let site = site_packages(venv).context("the venv has no site-packages")?;
    let mut scripts = Vec::new();
    for entry in std::fs::read_dir(&site)?.flatten() {
        let file = entry.file_name().to_string_lossy().into_owned();
        let Some(dist) = file.strip_suffix(".dist-info") else {
            continue;
        };
        let project = dist.rsplit_once('-').map_or(dist, |(p, _)| p);
        if normalize(project) != wanted {
            continue;
        }
        let points =
            std::fs::read_to_string(entry.path().join("entry_points.txt")).unwrap_or_default();
        let mut in_scripts = false;
        for line in points.lines().map(str::trim) {
            if line.starts_with('[') {
                in_scripts = line == "[console_scripts]";
            } else if in_scripts && let Some((script, _)) = line.split_once('=') {
                scripts.push(script.trim().to_string());
            }
        }
    }
    let script = scripts
        .iter()
        .find(|s| normalize(s) == wanted)
        .or(scripts.first())
        .with_context(|| format!("{name} declares no command"))?;
    Ok(if cfg!(windows) {
        venv.join("Scripts").join(format!("{script}.exe"))
    } else {
        venv.join("bin").join(script)
    })
}

fn site_packages(venv: &Path) -> Option<PathBuf> {
    if cfg!(windows) {
        return Some(venv.join("Lib").join("site-packages"));
    }
    std::fs::read_dir(venv.join("lib"))
        .ok()?
        .flatten()
        .map(|e| e.path().join("site-packages"))
        .find(|p| p.is_dir())
}

//installers run with the same scrubbed env
async fn run_tool(mut command: Command, home: &Path, path: &[PathBuf]) -> Result<()> {
    let base = scrubbed_env(home, path)?;
    let mut env: Vec<(OsString, OsString)> = base;
    for (key, value) in command.as_std().get_envs() {
        if let Some(value) = value {
            env.push((key.to_os_string(), value.to_os_string()));
        }
    }
    command
        .env_clear()
        .envs(env)
        .current_dir(home)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    let output = command.output().await.context("cannot run the installer")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let last: Vec<&str> = stderr.lines().rev().take(8).collect();
        bail!(
            "install failed: {}",
            last.into_iter().rev().collect::<Vec<_>>().join("\n")
        );
    }
    Ok(())
}

//stderr goes to the log and the tail
async fn keep_log(stderr: tokio::process::ChildStderr, server: Arc<Server>) {
    let file = server.dir.join(LOG_FILE);
    if std::fs::metadata(&file).is_ok_and(|m| m.len() > MAX_LOG_BYTES) {
        let _ = std::fs::rename(&file, file.with_extension("log.old"));
    }
    let mut log = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file)
        .await
        .ok();
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(log) = log.as_mut() {
            let _ = log.write_all(format!("{line}\n").as_bytes()).await;
        }
        let mut tail = server.tail.lock().unwrap();
        if tail.len() == TAIL_LINES {
            tail.pop_front();
        }
        tail.push_back(line);
    }
}

//exact versions only, ranges change by themselves
fn check_source(source: &Source) -> Result<()> {
    let version_ok = |v: &str| {
        v.starts_with(|c: char| c.is_ascii_digit())
            && v.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".+-!_".contains(&b))
    };
    match source {
        Source::Mcpb { url, sha256 } => {
            let url = reqwest::Url::parse(url).context("invalid bundle url")?;
            let local = matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
            ensure!(
                url.scheme() == "https" || (url.scheme() == "http" && local),
                "bundles download over https"
            );
            ensure!(
                sha256.len() == 64 && sha256.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid bundle checksum"
            );
        }
        Source::Npm { package, version } => {
            let part_ok = |p: &str| {
                !p.is_empty()
                    && !p.starts_with('.')
                    && p.bytes().all(|b| {
                        b.is_ascii_lowercase() || b.is_ascii_digit() || b"-._~".contains(&b)
                    })
            };
            let name_ok = match package.strip_prefix('@') {
                Some(scoped) => scoped
                    .split_once('/')
                    .is_some_and(|(s, n)| part_ok(s) && part_ok(n)),
                None => part_ok(package),
            };
            ensure!(
                name_ok && package.len() <= 214,
                "invalid npm package {package}"
            );
            ensure!(version_ok(version), "{version} is not an exact version");
        }
        Source::Pypi { package, version } => {
            let name_ok = !package.is_empty()
                && package
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._".contains(&b))
                && package.starts_with(|c: char| c.is_ascii_alphanumeric());
            ensure!(name_ok, "invalid python package {package}");
            ensure!(version_ok(version), "{version} is not an exact version");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn npm(package: &str, version: &str) -> Source {
        Source::Npm {
            package: package.into(),
            version: version.into(),
        }
    }

    #[test]
    fn sources_need_exact_versions() {
        assert!(check_source(&npm("@modelcontextprotocol/server-everything", "1.0.0")).is_ok());
        assert!(check_source(&npm("left-pad", "^1.0.0")).is_err());
        assert!(check_source(&npm("left-pad", "latest")).is_err());
        assert!(check_source(&npm("../evil", "1.0.0")).is_err());
        assert!(check_source(&npm("@scope", "1.0.0")).is_err());
        let pypi = |package: &str, version: &str| Source::Pypi {
            package: package.into(),
            version: version.into(),
        };
        assert!(check_source(&pypi("mcp-server-time", "2026.10.10")).is_ok());
        assert!(check_source(&pypi("mcp-server-time", ">=1")).is_err());
        assert!(check_source(&pypi("x; rm -rf", "1")).is_err());
        let bundle = |url: &str| Source::Mcpb {
            url: url.into(),
            sha256: "a".repeat(64),
        };
        assert!(check_source(&bundle("https://example.com/x.mcpb")).is_ok());
        assert!(check_source(&bundle("http://127.0.0.1:8080/x.mcpb")).is_ok());
        assert!(check_source(&bundle("http://example.com/x.mcpb")).is_err());
    }
}
