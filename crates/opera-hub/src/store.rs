use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use opera_hub_proto::{
    DeviceInfo, KIND_DESKTOP, KIND_PHONE, PairRejection, SCOPE_ADMIN, SCOPE_PROVIDERS,
    SCOPE_SECRETS, SCOPE_SYNC,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const DEVICES_FILE: &str = "devices.json";
const PAIRING_FILE: &str = "pairing.json";
pub const PAIRING_TTL: Duration = Duration::from_secs(10 * 60);

//devices.json is the allowlist
pub type Device = DeviceInfo;

//phones and desktops get every scope
pub fn default_scopes(kind: &str) -> Vec<String> {
    match kind {
        KIND_PHONE | KIND_DESKTOP => [SCOPE_SYNC, SCOPE_SECRETS, SCOPE_PROVIDERS, SCOPE_ADMIN]
            .map(String::from)
            .to_vec(),
        _ => Vec::new(),
    }
}

#[derive(Serialize, Deserialize)]
struct PendingPairing {
    secret: String,
    expires_at: u64,
}

//atomic read modify write
#[derive(Debug)]
pub struct Store {
    dir: PathBuf,
    lock: Mutex<()>,
}

impl Store {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            lock: Mutex::new(()),
        }
    }

    pub fn devices(&self) -> Result<Vec<Device>> {
        Ok(read_json(&self.dir.join(DEVICES_FILE))?.unwrap_or_default())
    }

    pub fn device(&self, id: &str) -> Result<Option<Device>> {
        Ok(self.devices()?.into_iter().find(|device| device.id == id))
    }

    pub fn touch(&self, id: &str) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        let mut devices = self.devices()?;
        let Some(device) = devices.iter_mut().find(|device| device.id == id) else {
            return Ok(());
        };
        device.last_seen = Some(unix_now());
        write_json(&self.dir.join(DEVICES_FILE), &devices)
    }

    pub fn remove_device(&self, id: &str) -> Result<bool> {
        let _guard = self.lock.lock().unwrap();
        let mut devices = self.devices()?;
        let before = devices.len();
        devices.retain(|device| device.id != id);
        if devices.len() == before {
            return Ok(false);
        }
        write_json(&self.dir.join(DEVICES_FILE), &devices)?;
        Ok(true)
    }

    //only the latest qr code works
    pub fn start_pairing(&self) -> Result<String> {
        let _guard = self.lock.lock().unwrap();
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("no system randomness: {e}"))?;
        let secret: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let pending = PendingPairing {
            secret: secret.clone(),
            expires_at: unix_now() + PAIRING_TTL.as_secs(),
        };
        write_json(&self.dir.join(PAIRING_FILE), &pending)?;
        Ok(secret)
    }

    pub fn finish_pairing(
        &self,
        secret: &str,
        device: Device,
    ) -> Result<Result<(), PairRejection>> {
        let _guard = self.lock.lock().unwrap();
        let path = self.dir.join(PAIRING_FILE);
        let Some(pending) = read_json::<PendingPairing>(&path)? else {
            return Ok(Err(PairRejection::Invalid));
        };
        if pending.expires_at < unix_now() {
            std::fs::remove_file(&path)?;
            return Ok(Err(PairRejection::Expired));
        }
        if !same_secret(&pending.secret, secret) {
            return Ok(Err(PairRejection::Invalid));
        }
        //one time secret
        std::fs::remove_file(&path)?;
        let mut devices = self.devices()?;
        devices.retain(|known| known.id != device.id);
        devices.push(device);
        write_json(&self.dir.join(DEVICES_FILE), &devices)?;
        Ok(Ok(()))
    }
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

//constant time against timing attacks
fn same_secret(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("invalid {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
    }
}

//rename so crashes never truncate
fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    use std::io::Write;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp)
        .with_context(|| format!("cannot write {}", tmp.display()))?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path).with_context(|| format!("cannot write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> Store {
        let dir = std::env::temp_dir().join(format!("opera-hub-store-{}", rand_suffix()));
        Store::new(&dir)
    }

    fn rand_suffix() -> String {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).unwrap();
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn device(id: &str) -> Device {
        Device {
            id: id.into(),
            name: "phone".into(),
            kind: KIND_PHONE.into(),
            scopes: default_scopes(KIND_PHONE),
            paired_at: 0,
            paired_by: None,
            last_seen: None,
        }
    }

    #[test]
    fn pairing_secret_works_once() {
        let store = temp_store();
        let secret = store.start_pairing().unwrap();
        assert_eq!(
            store.finish_pairing("wrong", device("a")).unwrap(),
            Err(PairRejection::Invalid)
        );
        assert_eq!(store.finish_pairing(&secret, device("a")).unwrap(), Ok(()));
        assert_eq!(
            store.finish_pairing(&secret, device("b")).unwrap(),
            Err(PairRejection::Invalid)
        );
        assert!(store.device("a").unwrap().is_some());
        assert!(!store.device("b").unwrap().is_some());
        assert!(store.remove_device("a").unwrap());
        assert!(!store.device("a").unwrap().is_some());
    }
}
