//! SQLite storage backend (S2), selected by `METALCRAFT_STORE=sqlite`.
//!
//! One database at `<data>/agent.db`, in WAL mode so a Litestream sidecar can stream the WAL to
//! R2 for durable, restore-on-boot state on an ephemeral-disk Cloudflare Container.
//!
//! Scope (S2): the **blob-per-entity** stores — chats and flow runs — live here. The
//! logic-bearing collection stores (scheduled/keys/gateway/packs) still carry their behavior in
//! their own modules and are delegated to the files backend for now (S2b/c will give those modules
//! a shared document-persistence seam so they too land in `agent.db`). Until then, `sqlite` mode is
//! a hybrid: chats + flow runs in SQLite, the rest as JSON files.

use std::path::PathBuf;
use std::sync::Mutex;

use rusqlite::Connection;

use super::{
    ChatStore, DocStore, FlowRunStore, GatewayStore, KeysStore, PackStore, ScheduledStore, Store,
};
use crate::flow_runs::FlowRun;
use crate::workshop_api::PersistedChat;

pub(super) fn default_db_path() -> PathBuf {
    crate::paths::data_dir().join("agent.db")
}

pub(super) struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub(super) fn open_default() -> rusqlite::Result<Self> {
        Self::open(default_db_path())
    }

    pub(super) fn open(path: PathBuf) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(&path)?;
        // WAL is required for Litestream. NORMAL sync + WAL is durable enough (Litestream ships
        // the WAL); busy_timeout avoids spurious SQLITE_BUSY under the single-writer model.
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA busy_timeout=5000;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
             CREATE TABLE IF NOT EXISTS chats(id TEXT PRIMARY KEY, body TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS flow_runs(id TEXT PRIMARY KEY, body TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS docs(name TEXT PRIMARY KEY, body TEXT NOT NULL);",
        )?;
        Ok(Self { conn: Mutex::new(conn) })
    }
}

impl Store for SqliteStore {
    fn chats(&self) -> &dyn ChatStore {
        self
    }
    fn flow_runs(&self) -> &dyn FlowRunStore {
        self
    }
    fn docs(&self) -> &dyn DocStore {
        self
    }
    // The collection stores keep their logic in their own modules; those modules persist through
    // `store().docs()`, which is *this* SqliteStore's DocStore in sqlite mode — so delegating the
    // facades to the files backend still lands the data in agent.db. No hybrid.
    fn scheduled(&self) -> &dyn ScheduledStore {
        super::files_backend().scheduled()
    }
    fn keys(&self) -> &dyn KeysStore {
        super::files_backend().keys()
    }
    fn gateway(&self) -> &dyn GatewayStore {
        super::files_backend().gateway()
    }
    fn packs(&self) -> &dyn PackStore {
        super::files_backend().packs()
    }
}

impl DocStore for SqliteStore {
    fn get(&self, name: &str) -> Option<String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT body FROM docs WHERE name=?1",
            rusqlite::params![name],
            |r| r.get::<_, String>(0),
        )
        .ok()
    }
    fn put(&self, name: &str, body: &str) -> std::io::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO docs(name, body) VALUES(?1, ?2)
             ON CONFLICT(name) DO UPDATE SET body=excluded.body",
            rusqlite::params![name, body],
        )
        .map_err(std::io::Error::other)?;
        Ok(())
    }
}

impl ChatStore for SqliteStore {
    fn save(&self, id: &str, chat: &PersistedChat) {
        let body = match serde_json::to_string(chat) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("failed to serialize chat {id}: {e}");
                return;
            }
        };
        let conn = self.conn.lock().unwrap();
        if let Err(e) = conn.execute(
            "INSERT INTO chats(id, body) VALUES(?1, ?2)
             ON CONFLICT(id) DO UPDATE SET body=excluded.body",
            rusqlite::params![id, body],
        ) {
            log::warn!("failed to persist chat {id}: {e}");
        }
    }

    fn load_all(&self) -> Vec<PersistedChat> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare("SELECT id, body FROM chats") {
            Ok(s) => s,
            Err(e) => {
                log::warn!("chats: prepare failed: {e}");
                return Vec::new();
            }
        };
        let rows = match stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        }) {
            Ok(it) => it,
            Err(e) => {
                log::warn!("chats: query failed: {e}");
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for row in rows {
            match row {
                Ok((id, body)) => match serde_json::from_str::<PersistedChat>(&body) {
                    Ok(pc) => out.push(pc),
                    Err(e) => log::warn!("chats: failed to parse chat {id}: {e}"),
                },
                Err(e) => log::warn!("chats: row error: {e}"),
            }
        }
        out
    }

    fn delete(&self, id: &str) {
        let conn = self.conn.lock().unwrap();
        if let Err(e) = conn.execute("DELETE FROM chats WHERE id=?1", rusqlite::params![id]) {
            log::warn!("failed to delete chat {id}: {e}");
        }
    }
}

impl FlowRunStore for SqliteStore {
    fn save(&self, run: &FlowRun) -> std::io::Result<()> {
        let body = serde_json::to_string(run).map_err(std::io::Error::other)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO flow_runs(id, body) VALUES(?1, ?2)
             ON CONFLICT(id) DO UPDATE SET body=excluded.body",
            rusqlite::params![run.id, body],
        )
        .map_err(std::io::Error::other)?;
        Ok(())
    }

    fn load(&self, id: &str) -> Option<FlowRun> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT body FROM flow_runs WHERE id=?1",
            rusqlite::params![id],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|body| serde_json::from_str(&body).ok())
    }

    fn list(&self) -> Vec<FlowRun> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare("SELECT id, body FROM flow_runs") {
            Ok(s) => s,
            Err(e) => {
                log::warn!("flow_runs: prepare failed: {e}");
                return Vec::new();
            }
        };
        let rows = match stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        }) {
            Ok(it) => it,
            Err(e) => {
                log::warn!("flow_runs: query failed: {e}");
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for row in rows {
            match row {
                Ok((id, body)) => match serde_json::from_str::<FlowRun>(&body) {
                    Ok(fr) => out.push(fr),
                    Err(e) => log::warn!("flow_runs: failed to parse run {id}: {e}"),
                },
                Err(e) => log::warn!("flow_runs: row error: {e}"),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, SqliteStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("agent.db")).unwrap();
        (dir, store)
    }

    #[test]
    fn chats_round_trip() {
        let (_dir, s) = temp_store();
        let chat: PersistedChat = serde_json::from_value(serde_json::json!({
            "id": "c1", "persona_slug": "p", "model_name": "m", "cwd": "/", "created_at": "t"
        }))
        .unwrap();

        assert!(ChatStore::load_all(&s).is_empty());
        ChatStore::save(&s, "c1", &chat);
        let all = ChatStore::load_all(&s);
        assert_eq!(all.len(), 1);
        // upsert (no duplicate row)
        ChatStore::save(&s, "c1", &chat);
        assert_eq!(ChatStore::load_all(&s).len(), 1);
        ChatStore::delete(&s, "c1");
        assert!(ChatStore::load_all(&s).is_empty());
    }

    #[test]
    fn flow_runs_round_trip() {
        let (_dir, s) = temp_store();
        let run: FlowRun = serde_json::from_value(serde_json::json!({
            "id": "r1", "flow_id": "f", "status": "paused", "current_node_id": "n",
            "variables": {}, "pause": null, "persona": "p", "model": "m", "cwd": "/",
            "steps": [], "flow": null, "created_at": "t", "updated_at": "t"
        }))
        .unwrap();

        assert!(FlowRunStore::list(&s).is_empty());
        FlowRunStore::save(&s, &run).unwrap();
        assert_eq!(FlowRunStore::list(&s).len(), 1);
        assert_eq!(FlowRunStore::load(&s, "r1").unwrap().id, "r1");
        assert!(FlowRunStore::load(&s, "missing").is_none());
    }

    #[test]
    fn docs_round_trip() {
        let (_dir, s) = temp_store();
        assert!(DocStore::get(&s, "scheduled_tasks").is_none());
        DocStore::put(&s, "scheduled_tasks", "[]").unwrap();
        assert_eq!(DocStore::get(&s, "scheduled_tasks").as_deref(), Some("[]"));
        // upsert replaces
        DocStore::put(&s, "scheduled_tasks", "[1]").unwrap();
        assert_eq!(DocStore::get(&s, "scheduled_tasks").as_deref(), Some("[1]"));
        // distinct names are independent
        assert!(DocStore::get(&s, "keys").is_none());
    }

    #[test]
    fn wal_mode_enabled() {
        let (_dir, s) = temp_store();
        let conn = s.conn.lock().unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
    }
}
