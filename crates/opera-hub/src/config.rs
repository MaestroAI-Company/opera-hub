use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use directories::ProjectDirs;
use iroh::{RelayMode, RelayUrl, SecretKey};
use serde::Deserialize;

const CONFIG_FILE: &str = "config.toml";
pub const SECRET_KEY_FILE: &str = "secret.key";

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    //empty means the public n0 relays
    pub relays: Vec<RelayUrl>,
    //fixed port for a firewall rule
    pub port: Option<u16>,
    //shown to devices hostname by default
    pub name: Option<String>,
}

impl Config {
    pub fn load(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(CONFIG_FILE);
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                toml::from_str(&text).with_context(|| format!("invalid {}", path.display()))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
        }
    }

    pub fn hub_name(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned())
    }

    pub fn relay_mode(&self) -> RelayMode {
        if self.relays.is_empty() {
            RelayMode::Default
        } else {
            RelayMode::custom(self.relays.clone())
        }
    }
}

pub fn default_data_dir() -> Result<PathBuf> {
    //local so the key never roams
    ProjectDirs::from("company", "Maestro AI", "Opera Hub")
        .map(|dirs| dirs.data_local_dir().to_path_buf())
        .context("no home directory to store hub data")
}

//losing it unpairs every device
pub fn load_or_create_key(data_dir: &Path) -> Result<SecretKey> {
    let path = data_dir.join(SECRET_KEY_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let bytes: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow!("corrupt key file {}", path.display()))?;
            Ok(SecretKey::from_bytes(&bytes))
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            std::fs::create_dir_all(data_dir)
                .with_context(|| format!("cannot create {}", data_dir.display()))?;
            let key = SecretKey::generate();
            if !write_private(&path, &key.to_bytes())? {
                //another process won the race
                return load_or_create_key(data_dir);
            }
            Ok(key)
        }
        Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
    }
}

//false if it exists never half written
pub(crate) fn write_private(path: &Path, bytes: &[u8]) -> Result<bool> {
    use std::io::Write;
    let mut suffix = [0u8; 8];
    getrandom::fill(&mut suffix).map_err(|e| anyhow!("no system randomness: {e}"))?;
    let suffix: String = suffix.iter().map(|b| format!("{b:02x}")).collect();
    let tmp = path.with_extension(format!("tmp{suffix}"));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp)
        .with_context(|| format!("cannot create {}", tmp.display()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    //linking fails atomically if the target exists
    let linked = std::fs::hard_link(&tmp, path);
    std::fs::remove_file(&tmp)?;
    match linked {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e).with_context(|| format!("cannot create {}", path.display())),
    }
}
