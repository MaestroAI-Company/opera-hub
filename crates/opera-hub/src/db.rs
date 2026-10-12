use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result, anyhow, bail};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use opera_hub_proto::{OAuthGrant, Record};
use rusqlite::{Connection, OptionalExtension, params};

const DB_FILE: &str = "hub.db";
const KEY_FILE: &str = "data.key";
const NONCE_BYTES: usize = 24;

//contents sealed per row
pub struct Db {
    conn: Mutex<Connection>,
    cipher: XChaCha20Poly1305,
}

impl Db {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let key = data_key(data_dir)?;
        Self::open_with_key(&data_dir.join(DB_FILE), &key)
    }

    pub fn open_with_key(path: &Path, key: &[u8; 32]) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn =
            Connection::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS records (
                collection TEXT NOT NULL,
                id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                device TEXT NOT NULL,
                deleted INTEGER NOT NULL,
                payload BLOB,
                PRIMARY KEY (collection, id)
            );
            CREATE INDEX IF NOT EXISTS records_seq ON records (collection, seq);
            CREATE TABLE IF NOT EXISTS grants (
                server_id TEXT PRIMARY KEY,
                grant BLOB NOT NULL
            );",
        )?;
        let cipher = XChaCha20Poly1305::new_from_slice(key).map_err(|_| anyhow!("bad data key"))?;
        Ok(Self {
            conn: Mutex::new(conn),
            cipher,
        })
    }

    pub fn push(&self, collection: &str, device: &str, records: &[Record]) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        for record in records {
            let current: Option<(u64, String)> = tx
                .query_row(
                    "SELECT updated_at, device FROM records WHERE collection = ?1 AND id = ?2",
                    params![collection, record.id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            //ties go to the higher device id
            if let Some(current) = current
                && (current.0, current.1.as_str()) >= (record.updated_at, device)
            {
                continue;
            }
            let seq: u64 =
                tx.query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM records", [], |row| {
                    row.get(0)
                })?;
            let payload = match (&record.payload, record.deleted) {
                (Some(payload), false) => {
                    Some(self.seal(&aad(collection, &record.id), payload.as_bytes())?)
                }
                _ => None,
            };
            tx.execute(
                "INSERT OR REPLACE INTO records (collection, id, seq, updated_at, device, deleted, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![collection, record.id, seq, record.updated_at, device, record.deleted, payload],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    //last seq becomes the next cursor
    pub fn pull(
        &self,
        collection: &str,
        since: u64,
        limit: usize,
    ) -> Result<(Vec<Record>, u64, bool)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, seq, updated_at, deleted, payload FROM records
             WHERE collection = ?1 AND seq > ?2 ORDER BY seq LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![collection, since, limit as i64 + 1], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, u64>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, Option<Vec<u8>>>(4)?,
            ))
        })?;
        let mut records = Vec::new();
        let mut cursor = since;
        let mut more = false;
        for row in rows {
            let (id, seq, updated_at, deleted, sealed) = row?;
            if records.len() == limit {
                more = true;
                break;
            }
            let payload = match sealed {
                Some(sealed) => Some(String::from_utf8(
                    self.unseal(&aad(collection, &id), &sealed)?,
                )?),
                None => None,
            };
            records.push(Record {
                id,
                updated_at,
                deleted,
                payload,
            });
            cursor = seq;
        }
        Ok((records, cursor, more))
    }

    pub fn put_grant(&self, grant: &OAuthGrant) -> Result<()> {
        let sealed = self.seal(
            &aad("grants", &grant.server_id),
            &serde_json::to_vec(grant)?,
        )?;
        self.conn.lock().unwrap().execute(
            "INSERT OR REPLACE INTO grants (server_id, grant) VALUES (?1, ?2)",
            params![grant.server_id, sealed],
        )?;
        Ok(())
    }

    pub fn grant(&self, server_id: &str) -> Result<Option<OAuthGrant>> {
        let sealed: Option<Vec<u8>> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT grant FROM grants WHERE server_id = ?1",
                params![server_id],
                |row| row.get(0),
            )
            .optional()?;
        match sealed {
            Some(sealed) => Ok(Some(serde_json::from_slice(
                &self.unseal(&aad("grants", server_id), &sealed)?,
            )?)),
            None => Ok(None),
        }
    }

    pub fn drop_grant(&self, server_id: &str) -> Result<()> {
        self.conn.lock().unwrap().execute(
            "DELETE FROM grants WHERE server_id = ?1",
            params![server_id],
        )?;
        Ok(())
    }

    fn seal(&self, aad: &[u8], plain: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE_BYTES];
        getrandom::fill(&mut nonce).map_err(|e| anyhow!("no system randomness: {e}"))?;
        let sealed = self
            .cipher
            .encrypt(&XNonce::from(nonce), Payload { msg: plain, aad })
            .map_err(|_| anyhow!("encryption failed"))?;
        Ok([nonce.as_slice(), &sealed].concat())
    }

    fn unseal(&self, aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>> {
        if sealed.len() < NONCE_BYTES {
            bail!("corrupt row");
        }
        let (nonce, sealed) = sealed.split_at(NONCE_BYTES);
        let nonce: [u8; NONCE_BYTES] = nonce.try_into()?;
        self.cipher
            .decrypt(&XNonce::from(nonce), Payload { msg: sealed, aad })
            .map_err(|_| anyhow!("cannot decrypt row, wrong data key"))
    }
}

//binds ciphertext to its row
fn aad(collection: &str, id: &str) -> Vec<u8> {
    [collection.as_bytes(), b"\0", id.as_bytes()].concat()
}

//os keychain or a key file
fn data_key(data_dir: &Path) -> Result<[u8; 32]> {
    let file = data_dir.join(KEY_FILE);
    if file.exists() {
        let bytes = std::fs::read(&file)?;
        return bytes
            .try_into()
            .map_err(|_| anyhow!("corrupt {}", file.display()));
    }
    #[cfg(any(windows, target_os = "macos"))]
    match keychain_key(data_dir) {
        Ok(key) => return Ok(key),
        //a new key would lock data away
        Err(e) if data_dir.join(DB_FILE).exists() => {
            bail!("os keychain unavailable and {DB_FILE} already exists: {e}")
        }
        Err(e) => tracing::warn!("os keychain unavailable, using {}: {e}", file.display()),
    }
    let mut key = [0u8; 32];
    getrandom::fill(&mut key).map_err(|e| anyhow!("no system randomness: {e}"))?;
    if !crate::config::write_private(&file, &key)? {
        return data_key(data_dir);
    }
    Ok(key)
}

#[cfg(any(windows, target_os = "macos"))]
fn keychain_key(data_dir: &Path) -> Result<[u8; 32]> {
    let entry = keyring::Entry::new("opera-hub", &data_dir.display().to_string())?;
    match entry.get_secret() {
        Ok(bytes) => bytes
            .try_into()
            .map_err(|_| anyhow!("corrupt keychain entry")),
        Err(keyring::Error::NoEntry) => {
            let mut key = [0u8; 32];
            getrandom::fill(&mut key).map_err(|e| anyhow!("no system randomness: {e}"))?;
            entry.set_secret(&key)?;
            Ok(key)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> Db {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).unwrap();
        let suffix: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let path = std::env::temp_dir()
            .join(format!("opera-hub-db-{suffix}"))
            .join(DB_FILE);
        Db::open_with_key(&path, &[7u8; 32]).unwrap()
    }

    fn record(id: &str, updated_at: u64, payload: &str) -> Record {
        Record {
            id: id.into(),
            updated_at,
            deleted: false,
            payload: Some(payload.into()),
        }
    }

    #[test]
    fn last_writer_wins() {
        let db = temp_db();
        db.push("providers", "a", &[record("x", 10, "new")])
            .unwrap();
        db.push("providers", "b", &[record("x", 5, "old")]).unwrap();
        let (records, cursor, more) = db.pull("providers", 0, 10).unwrap();
        assert_eq!(records, vec![record("x", 10, "new")]);
        assert!(!more);

        let tombstone = Record {
            id: "x".into(),
            updated_at: 20,
            deleted: true,
            payload: None,
        };
        db.push("providers", "b", std::slice::from_ref(&tombstone))
            .unwrap();
        let (records, _, _) = db.pull("providers", cursor, 10).unwrap();
        assert_eq!(records, vec![tombstone]);
    }

    #[test]
    fn pull_pages_by_cursor() {
        let db = temp_db();
        let records: Vec<_> = (0..5).map(|i| record(&format!("r{i}"), 1, "p")).collect();
        db.push("providers", "a", &records).unwrap();
        let (page, cursor, more) = db.pull("providers", 0, 3).unwrap();
        assert_eq!((page.len(), more), (3, true));
        let (page, _, more) = db.pull("providers", cursor, 3).unwrap();
        assert_eq!((page.len(), more), (2, false));
    }

    #[test]
    fn payloads_are_sealed() {
        let db = temp_db();
        db.push("providers", "a", &[record("x", 1, "sk-secret-key")])
            .unwrap();
        let raw: Vec<u8> = db
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT payload FROM records", [], |row| row.get(0))
            .unwrap();
        assert!(!raw.windows(13).any(|w| w == b"sk-secret-key"));
    }
}
