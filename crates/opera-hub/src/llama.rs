//llama.cpp on the hub, one llama-server in router mode
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use opera_hub_proto::{
    CatalogModel, HubUsage, InstalledModel, LlamaStatus, ModelDownload, ModelFile, ModelRepo,
    ModelsRequest, ModelsResponse, SCOPE_ADMIN, read_frame, write_frame,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::protocols::{Access, Peer, Protocol};

pub const PROVIDER_ID: &str = "llamacpp";
const RELEASES_API: &str = "https://api.github.com/repos/ggml-org/llama.cpp/releases";
const HF: &str = "https://huggingface.co";
//one model in memory, the router swaps on demand
const MODELS_MAX: &str = "1";
//companion files are not chat models
const COMPANIONS: [&str; 5] = ["mmproj-", "mtp-", "dflash-", "eagle3-", "draft-"];
const SEARCH_LIMIT: &str = "20";
const START_TIMEOUT: Duration = Duration::from_secs(60);
const START_POLL: Duration = Duration::from_millis(300);

//official builds from the model makers, smallest first
const CATALOG: [(&str, &str, &str, u64); 12] = [
    (
        "ggml-org/Qwen3.5-0.8B-GGUF",
        "Qwen3.5-0.8B-Q4_0.gguf",
        "Qwen3.5 0.8B",
        563_036_064,
    ),
    (
        "LiquidAI/LFM2.5-1.2B-Instruct-GGUF",
        "LFM2.5-1.2B-Instruct-Q4_K_M.gguf",
        "LFM2.5 1.2B",
        730_895_168,
    ),
    (
        "mistralai/Ministral-3-3B-Instruct-2512-GGUF",
        "Ministral-3-3B-Instruct-2512-Q4_K_M.gguf",
        "Ministral 3 3B",
        2_147_023_008,
    ),
    (
        "ggml-org/gemma-4-E2B-it-GGUF",
        "gemma-4-E2B-it-Q4_0.gguf",
        "Gemma 4 E2B",
        2_840_000_000,
    ),
    (
        "ggml-org/gemma-4-E4B-it-GGUF",
        "gemma-4-E4B-it-Q4_0.gguf",
        "Gemma 4 E4B",
        4_590_000_000,
    ),
    (
        "mistralai/Ministral-3-8B-Instruct-2512-GGUF",
        "Ministral-3-8B-Instruct-2512-Q4_K_M.gguf",
        "Ministral 3 8B",
        5_198_911_904,
    ),
    (
        "ibm-granite/granite-4.2-8b-GGUF",
        "granite-4.2-8b-Q4_K_M.gguf",
        "Granite 4.2 8B",
        5_347_917_952,
    ),
    (
        "ggml-org/gemma-4-12B-it-GGUF",
        "gemma-4-12B-it-Q4_0.gguf",
        "Gemma 4 12B",
        7_220_000_000,
    ),
    (
        "mistralai/Ministral-3-14B-Instruct-2512-GGUF",
        "Ministral-3-14B-Instruct-2512-Q4_K_M.gguf",
        "Ministral 3 14B",
        8_239_593_024,
    ),
    (
        "ggml-org/gemma-4-26B-A4B-it-GGUF",
        "gemma-4-26B-A4B-it-Q4_0.gguf",
        "Gemma 4 26B A4B",
        14_620_000_000,
    ),
    (
        "ggml-org/Qwen3.8-27B-GGUF",
        "Qwen3.8-27B-Q4_K_M.gguf",
        "Qwen3.8 27B",
        18_970_000_000,
    ),
    (
        "ggml-org/Qwen3.6-35B-A3B-GGUF",
        "Qwen3.6-35B-A3B-Q4_K_M.gguf",
        "Qwen3.6 35B A3B",
        20_420_000_000,
    ),
];

pub fn catalog() -> Vec<CatalogModel> {
    CATALOG
        .iter()
        .map(|(repo, file, name, size)| CatalogModel {
            repo: repo.to_string(),
            file: file.to_string(),
            name: name.to_string(),
            size: *size,
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Idle,
    Installing,
    Running,
    Failed,
}

impl State {
    fn as_str(self) -> &'static str {
        match self {
            State::Idle => "idle",
            State::Installing => "installing",
            State::Running => "running",
            State::Failed => "failed",
        }
    }
}

struct Download {
    repo: String,
    received: u64,
    total: u64,
    error: Option<String>,
    task: Option<JoinHandle<()>>,
}

struct Inner {
    state: State,
    backend: Option<String>,
    message: Option<String>,
    port: Option<u16>,
    child: Option<Child>,
    downloads: HashMap<String, Download>,
}

pub struct Llama {
    dir: PathBuf,
    http: reqwest::Client,
    inner: Mutex<Inner>,
    //one restart at a time
    restarting: tokio::sync::Mutex<()>,
    //the hub wide models setting
    enabled: AtomicBool,
    usage: crate::usage::Usage,
}

impl std::fmt::Debug for Llama {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Llama")
    }
}

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
    //sha256:hex from github
    digest: Option<String>,
}

#[derive(Deserialize)]
struct HfModel {
    id: String,
    #[serde(default)]
    downloads: u64,
}

#[derive(Deserialize)]
struct HfEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
    #[serde(default)]
    size: u64,
}

//a build of llama-server for one accelerator
struct Flavor {
    backend: &'static str,
    archive: &'static str,
    //cuda builds ship the runtime apart
    runtime: Option<&'static str>,
}

impl Llama {
    pub fn new(data_dir: &Path, http: reqwest::Client) -> Arc<Self> {
        Arc::new(Self {
            dir: data_dir.join("llama"),
            http,
            inner: Mutex::new(Inner {
                state: State::Idle,
                backend: None,
                message: None,
                port: None,
                child: None,
                downloads: HashMap::new(),
            }),
            restarting: tokio::sync::Mutex::new(()),
            enabled: AtomicBool::new(true),
            usage: Default::default(),
        })
    }

    //off stops the server until turned back on
    pub fn set_enabled(self: &Arc<Self>, on: bool) {
        self.enabled.store(on, Ordering::SeqCst);
        self.refresh();
    }

    fn models_dir(&self) -> PathBuf {
        self.dir.join("models")
    }

    //repo each downloaded file came from
    fn sources_path(&self) -> PathBuf {
        self.dir.join("sources.json")
    }

    fn sources(&self) -> HashMap<String, String> {
        std::fs::read(self.sources_path())
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    fn set_source(&self, file: &str, repo: Option<&str>) -> Result<()> {
        //the inner lock serializes writers
        let _inner = self.inner.lock().unwrap();
        let mut sources = self.sources();
        match repo {
            Some(repo) => sources.insert(file.to_string(), repo.to_string()),
            None => sources.remove(file),
        };
        std::fs::write(self.sources_path(), serde_json::to_vec(&sources)?)?;
        Ok(())
    }

    //the openai endpoint once a model can answer
    pub fn base_url(&self) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        match (inner.state, inner.port) {
            (State::Running, Some(port)) => Some(format!("http://127.0.0.1:{port}/v1")),
            _ => None,
        }
    }

    pub fn installed(&self) -> Vec<InstalledModel> {
        let Ok(entries) = std::fs::read_dir(self.models_dir()) else {
            return Vec::new();
        };
        let mut sources = self.sources();
        let mut models: Vec<InstalledModel> = entries
            .flatten()
            .filter_map(|entry| {
                let file = entry.file_name().to_string_lossy().into_owned();
                let size = entry.metadata().ok()?.len();
                let repo = sources.remove(&file);
                file.ends_with(".gguf")
                    .then_some(InstalledModel { file, size, repo })
            })
            .collect();
        models.sort_by(|a, b| a.file.cmp(&b.file));
        models
    }

    pub fn status(&self) -> LlamaStatus {
        let installed = self.installed();
        let inner = self.inner.lock().unwrap();
        let mut downloads: Vec<ModelDownload> = inner
            .downloads
            .iter()
            .map(|(file, d)| ModelDownload {
                repo: d.repo.clone(),
                file: file.clone(),
                received: d.received,
                total: d.total,
                error: d.error.clone(),
            })
            .collect();
        downloads.sort_by(|a, b| a.file.cmp(&b.file));
        let pid = inner.child.as_ref().and_then(|child| child.id());
        let status = LlamaStatus {
            state: inner.state.as_str().into(),
            backend: inner.backend.clone(),
            message: inner.message.clone(),
            installed,
            downloads,
            usage: None,
        };
        drop(inner);
        LlamaStatus {
            usage: pid
                .and_then(|pid| self.usage.measure(&[pid]).into_iter().next())
                .map(|load| HubUsage {
                    cpu: load.cpu,
                    memory: load.memory,
                }),
            ..status
        }
    }

    fn set_state(&self, state: State, message: Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        inner.state = state;
        inner.message = message;
    }

    //restarts the server so it sees the models on disk
    pub fn refresh(self: &Arc<Self>) {
        let llama = self.clone();
        tokio::spawn(async move {
            let _one = llama.restarting.lock().await;
            llama.stop_server();
            let enabled = llama.enabled.load(Ordering::SeqCst);
            if !enabled || llama.installed().is_empty() {
                llama.set_state(State::Idle, None);
                return;
            }
            if let Err(e) = llama.install_and_start().await {
                warn!("llama.cpp did not start: {e:#}");
                llama.set_state(State::Failed, Some(format!("{e:#}")));
            }
        });
    }

    pub fn stop_server(&self) {
        let mut inner = self.inner.lock().unwrap();
        //kill on drop ends the process
        inner.child = None;
        inner.port = None;
        if inner.state == State::Running {
            inner.state = State::Idle;
        }
    }

    async fn install_and_start(&self) -> Result<()> {
        self.set_state(State::Installing, None);
        let flavors = flavors();
        let mut last = anyhow!("no llama.cpp build for this computer");
        for flavor in &flavors {
            let exe = match self.install(flavor).await {
                Ok(exe) => exe,
                Err(e) => {
                    warn!("llama.cpp {} build unavailable: {e:#}", flavor.backend);
                    last = e;
                    continue;
                }
            };
            //a gpu build without a working driver falls back
            match self.start(&exe).await {
                Ok(port) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.backend = Some(flavor.backend.into());
                    inner.port = Some(port);
                    inner.state = State::Running;
                    inner.message = None;
                    info!("llama.cpp runs on {} at port {port}", flavor.backend);
                    return Ok(());
                }
                Err(e) => {
                    warn!("llama.cpp {} build failed to start: {e:#}", flavor.backend);
                    last = e;
                }
            }
        }
        Err(last)
    }

    //the stable release names the build to use
    async fn install(&self, flavor: &Flavor) -> Result<PathBuf> {
        let stable: Release = self.github(&format!("{RELEASES_API}/latest")).await?;
        let pointer = stable
            .assets
            .iter()
            .find(|a| a.name == "nightly-tag.txt")
            .context("the llama.cpp release names no build")?;
        let build = self.download_verified(pointer).await?;
        let build = String::from_utf8(build)?.trim().to_string();
        ensure!(
            build.bytes().all(|b| b.is_ascii_alphanumeric()),
            "unexpected llama.cpp build {build}"
        );
        let target = self
            .dir
            .join("server")
            .join(format!("{build}-{}", flavor.backend));
        if let Some(exe) = find_server(&target) {
            return Ok(exe);
        }

        let release: Release = self.github(&format!("{RELEASES_API}/tags/{build}")).await?;
        let pick = |prefix: &str, suffix: &str| {
            release
                .assets
                .iter()
                .find(|a| a.name.starts_with(prefix) && a.name.ends_with(suffix))
        };
        let archive = pick("llama-", flavor.archive)
            .with_context(|| format!("{} has no {} build", release.tag_name, flavor.backend))?;
        let staging = target.with_extension("partial");
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging)?;
        info!("installing llama.cpp {build} for {}", flavor.backend);
        extract(
            &archive.name,
            &self.download_verified(archive).await?,
            &staging,
        )?;
        if let Some(runtime) = flavor.runtime {
            let runtime = pick("cudart-", runtime).context("the cuda runtime is missing")?;
            extract(
                &runtime.name,
                &self.download_verified(runtime).await?,
                &staging,
            )?;
        }
        let _ = std::fs::remove_dir_all(&target);
        std::fs::rename(&staging, &target)?;
        find_server(&target).context("the llama.cpp build has no llama-server")
    }

    async fn github<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        Ok(self
            .http
            .get(url)
            .header("user-agent", "opera-hub")
            .header("accept", "application/vnd.github+json")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    //github publishes a sha256 for every asset
    async fn download_verified(&self, asset: &Asset) -> Result<Vec<u8>> {
        let expected = asset
            .digest
            .as_deref()
            .and_then(|d| d.strip_prefix("sha256:"))
            .with_context(|| format!("{} has no checksum", asset.name))?
            .to_ascii_lowercase();
        let bytes = self
            .http
            .get(&asset.browser_download_url)
            .header("user-agent", "opera-hub")
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        ensure!(
            hex_sha256(&bytes) == expected,
            "checksum mismatch for {}",
            asset.name
        );
        Ok(bytes.to_vec())
    }

    async fn start(&self, exe: &Path) -> Result<u16> {
        let models = self.models_dir();
        std::fs::create_dir_all(&models)?;
        let port = free_port()?;
        let log = std::fs::File::create(self.dir.join("llama-server.log"))?;
        let mut command = Command::new(exe);
        command
            .arg("--models-dir")
            .arg(&models)
            .args(["--host", "127.0.0.1", "--port", &port.to_string()])
            .args(["--no-webui", "--models-max", MODELS_MAX])
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true);
        #[cfg(windows)]
        command.creation_flags(0x0800_0000);
        let mut child = command.spawn()?;

        let url = format!("http://127.0.0.1:{port}/v1/models");
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        loop {
            if let Some(status) = child.try_wait()? {
                bail!("llama-server exited with {status}, see llama-server.log");
            }
            let ready = self.http.get(&url).send().await;
            if ready.is_ok_and(|r| r.status().is_success()) {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "llama-server did not answer"
            );
            tokio::time::sleep(START_POLL).await;
        }
        self.inner.lock().unwrap().child = Some(child);
        Ok(port)
    }

    pub async fn search(&self, query: &str) -> Result<Vec<ModelRepo>> {
        let url = reqwest::Url::parse_with_params(
            &format!("{HF}/api/models"),
            &[
                ("search", query),
                ("filter", "gguf"),
                ("sort", "downloads"),
                ("limit", SEARCH_LIMIT),
            ],
        )?;
        let models: Vec<HfModel> = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(models
            .into_iter()
            .map(|m| ModelRepo {
                repo: m.id,
                downloads: m.downloads,
            })
            .collect())
    }

    //chat models only, split files are left out
    pub async fn files(&self, repo: &str) -> Result<Vec<ModelFile>> {
        check_repo(repo)?;
        let entries: Vec<HfEntry> = self
            .http
            .get(format!("{HF}/api/models/{repo}/tree/main"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(entries
            .into_iter()
            .filter(|e| e.kind == "file" && check_file(&e.path).is_ok())
            .filter(|e| !COMPANIONS.iter().any(|p| e.path.starts_with(p)))
            //some repos suffix the vision projector
            .filter(|e| !e.path.contains("-mmproj"))
            .filter(|e| !e.path.contains("-of-0"))
            .map(|e| ModelFile {
                file: e.path,
                size: e.size,
            })
            .collect())
    }

    pub fn download(self: &Arc<Self>, repo: String, file: String) -> Result<()> {
        check_repo(&repo)?;
        check_file(&file)?;
        let mut inner = self.inner.lock().unwrap();
        if inner
            .downloads
            .get(&file)
            .is_some_and(|d| d.error.is_none())
        {
            return Ok(());
        }
        let llama = self.clone();
        let task = tokio::spawn({
            let (repo, file) = (repo.clone(), file.clone());
            async move {
                match llama.fetch(&repo, &file).await {
                    Ok(()) => {
                        llama.inner.lock().unwrap().downloads.remove(&file);
                        info!("downloaded {file}");
                        llama.refresh();
                    }
                    Err(e) => {
                        warn!("download of {file} failed: {e:#}");
                        if let Some(d) = llama.inner.lock().unwrap().downloads.get_mut(&file) {
                            d.error = Some(format!("{e:#}"));
                        }
                    }
                }
            }
        });
        inner.downloads.insert(
            file,
            Download {
                repo,
                received: 0,
                total: 0,
                error: None,
                task: Some(task),
            },
        );
        Ok(())
    }

    //resumes a partial file left by a restart
    async fn fetch(&self, repo: &str, file: &str) -> Result<()> {
        let models = self.models_dir();
        std::fs::create_dir_all(&models)?;
        let part = models.join(format!("{file}.part"));
        let mut have = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
        let mut request = self.http.get(format!("{HF}/{repo}/resolve/main/{file}"));
        if have > 0 {
            request = request.header("range", format!("bytes={have}-"));
        }
        let mut response = request.send().await?.error_for_status()?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            have = 0;
        }
        let total = have + response.content_length().unwrap_or(0);
        let mut out = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(have > 0)
            .truncate(have == 0)
            .open(&part)
            .await?;
        let mut received = have;
        self.progress(file, received, total);
        while let Some(chunk) = response.chunk().await? {
            out.write_all(&chunk).await?;
            received += chunk.len() as u64;
            self.progress(file, received, total);
        }
        out.flush().await?;
        drop(out);
        ensure!(total == 0 || received == total, "the download ended early");
        self.set_source(file, Some(repo))?;
        std::fs::rename(&part, models.join(file))?;
        Ok(())
    }

    fn progress(&self, file: &str, received: u64, total: u64) {
        if let Some(d) = self.inner.lock().unwrap().downloads.get_mut(file) {
            d.received = received;
            d.total = total;
        }
    }

    pub fn cancel(&self, file: &str) -> Result<()> {
        check_file(file)?;
        if let Some(d) = self.inner.lock().unwrap().downloads.remove(file)
            && let Some(task) = d.task
        {
            task.abort();
        }
        let _ = std::fs::remove_file(self.models_dir().join(format!("{file}.part")));
        Ok(())
    }

    pub fn delete(self: &Arc<Self>, file: &str) -> Result<()> {
        check_file(file)?;
        //windows keeps a loaded model locked
        self.stop_server();
        std::fs::remove_file(self.models_dir().join(file))
            .with_context(|| format!("cannot delete {file}"))?;
        self.set_source(file, None)?;
        self.refresh();
        Ok(())
    }
}

//best accelerator first, the cpu build always works
fn flavors() -> Vec<Flavor> {
    let mut list = Vec::new();
    let flavor = |backend, archive, runtime| Flavor {
        backend,
        archive,
        runtime,
    };
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => {
            if Path::new(r"C:\Windows\System32\nvcuda.dll").exists() {
                list.push(flavor(
                    "cuda",
                    "-bin-win-cuda-12.4-x64.zip",
                    Some("-bin-win-cuda-12.4-x64.zip"),
                ));
            }
            if Path::new(r"C:\Windows\System32\vulkan-1.dll").exists() {
                list.push(flavor("vulkan", "-bin-win-vulkan-x64.zip", None));
            }
            list.push(flavor("cpu", "-bin-win-cpu-x64.zip", None));
        }
        ("linux", "x86_64") => {
            if Path::new("/proc/driver/nvidia/version").exists() {
                list.push(flavor(
                    "cuda",
                    "-bin-ubuntu-cuda-12.8-x64.tar.gz",
                    Some("-bin-ubuntu-cuda-12.8-x64.tar.gz"),
                ));
            }
            if has_vulkan_linux() {
                list.push(flavor("vulkan", "-bin-ubuntu-vulkan-x64.tar.gz", None));
            }
            list.push(flavor("cpu", "-bin-ubuntu-x64.tar.gz", None));
        }
        ("linux", "aarch64") => {
            if has_vulkan_linux() {
                list.push(flavor("vulkan", "-bin-ubuntu-vulkan-arm64.tar.gz", None));
            }
            list.push(flavor("cpu", "-bin-ubuntu-arm64.tar.gz", None));
        }
        ("macos", "aarch64") => list.push(flavor("metal", "-bin-macos-arm64.tar.gz", None)),
        ("macos", "x86_64") => list.push(flavor("cpu", "-bin-macos-x64.tar.gz", None)),
        _ => {}
    }
    list
}

fn has_vulkan_linux() -> bool {
    [
        "/usr/lib64/libvulkan.so.1",
        "/usr/lib/x86_64-linux-gnu/libvulkan.so.1",
        "/usr/lib/aarch64-linux-gnu/libvulkan.so.1",
        "/usr/lib/libvulkan.so.1",
    ]
    .iter()
    .any(|path| Path::new(path).exists())
}

pub(crate) fn extract(name: &str, bytes: &[u8], into: &Path) -> Result<()> {
    if name.ends_with(".zip") {
        zip::ZipArchive::new(std::io::Cursor::new(bytes))?.extract(into)?;
    } else {
        tar::Archive::new(flate2::read::GzDecoder::new(bytes)).unpack(into)?;
    }
    Ok(())
}

//archives may nest the binaries in a folder
fn find_server(dir: &Path) -> Option<PathBuf> {
    let name = if cfg!(windows) {
        "llama-server.exe"
    } else {
        "llama-server"
    };
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|f| f == name) {
                return Some(path);
            }
        }
    }
    None
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

pub(crate) fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

//owner/name from hugging face, nothing else
fn check_repo(repo: &str) -> Result<()> {
    let ok_part = |p: &str| {
        !p.is_empty()
            && !p.starts_with('.')
            && p.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    };
    match repo.split_once('/') {
        Some((owner, name)) if ok_part(owner) && ok_part(name) => Ok(()),
        _ => Err(anyhow!("invalid model repository {repo}")),
    }
}

//a plain gguf file name, never a path
fn check_file(file: &str) -> Result<()> {
    let plain = !file.starts_with('.')
        && file
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b));
    if plain && file.ends_with(".gguf") {
        Ok(())
    } else {
        Err(anyhow!("invalid model file {file}"))
    }
}

//admins manage the models from their devices
#[derive(Clone)]
pub struct Models {
    pub llama: Arc<Llama>,
}

impl std::fmt::Debug for Models {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Models")
    }
}

impl Protocol for Models {
    const ACCESS: Access = Access::Scope(SCOPE_ADMIN);

    async fn serve<R, W>(&self, _peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match self.handle(request).await {
                Ok(response) => response,
                Err(e) => ModelsResponse::Error {
                    message: format!("{e:#}"),
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Models {
    async fn handle(&self, request: ModelsRequest) -> Result<ModelsResponse> {
        let status = |llama: &Llama| ModelsResponse::Status {
            status: llama.status(),
        };
        Ok(match request {
            ModelsRequest::Catalog => ModelsResponse::Catalog { models: catalog() },
            ModelsRequest::Search { query } => ModelsResponse::Repos {
                repos: self.llama.search(&query).await?,
            },
            ModelsRequest::Files { repo } => ModelsResponse::Files {
                files: self.llama.files(&repo).await?,
            },
            ModelsRequest::Download { repo, file } => {
                self.llama.download(repo, file)?;
                status(&self.llama)
            }
            ModelsRequest::Cancel { file } => {
                self.llama.cancel(&file)?;
                status(&self.llama)
            }
            ModelsRequest::Delete { file } => {
                self.llama.delete(&file)?;
                status(&self.llama)
            }
            ModelsRequest::Status => status(&self.llama),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_stay_inside_the_models_dir() {
        assert!(check_file("Qwen3.5-0.8B-Q4_0.gguf").is_ok());
        assert!(check_file("../evil.gguf").is_err());
        assert!(check_file("dir/model.gguf").is_err());
        assert!(check_file("model.bin").is_err());
        assert!(check_repo("ggml-org/gemma-4-E2B-it-GGUF").is_ok());
        assert!(check_repo("ggml-org/../x").is_err());
        assert!(check_repo("noslash").is_err());
    }

    #[test]
    fn catalog_files_are_valid() {
        for model in catalog() {
            assert!(check_repo(&model.repo).is_ok(), "{}", model.repo);
            assert!(check_file(&model.file).is_ok(), "{}", model.file);
        }
    }
}
