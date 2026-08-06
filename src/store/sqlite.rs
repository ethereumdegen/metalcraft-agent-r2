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
use crate::session_io::SessionPreset;
use crate::workshop_api::{ChatMessageWire, PersistedChat};

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
             CREATE TABLE IF NOT EXISTS chats(
                 id TEXT PRIMARY KEY, persona_slug TEXT, model_name TEXT,
                 cwd TEXT, created_at TEXT, preset TEXT
             );
             CREATE TABLE IF NOT EXISTS messages(
                 chat_id TEXT NOT NULL, seq INTEGER NOT NULL, body TEXT NOT NULL,
                 PRIMARY KEY(chat_id, seq)
             );
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
    fn replace(&self, id: &str, chat: &PersistedChat) {
        let preset = serde_json::to_string(&chat.preset).unwrap_or_else(|_| "null".into());
        let mut guard = self.conn.lock().unwrap();
        let tx = match guard.transaction() {
            Ok(t) => t,
            Err(e) => {
                log::warn!("replace chat {id}: begin failed: {e}");
                return;
            }
        };
        let _ = tx.execute("DELETE FROM messages WHERE chat_id=?1", rusqlite::params![id]);
        let _ = tx.execute(
            "INSERT INTO chats(id, persona_slug, model_name, cwd, created_at, preset)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(id) DO UPDATE SET persona_slug=excluded.persona_slug,
                 model_name=excluded.model_name, cwd=excluded.cwd,
                 created_at=excluded.created_at, preset=excluded.preset",
            rusqlite::params![id, chat.persona_slug, chat.model_name, chat.cwd, chat.created_at, preset],
        );
        for (i, m) in chat.messages.iter().enumerate() {
            match serde_json::to_string(m) {
                Ok(body) => {
                    let _ = tx.execute(
                        "INSERT INTO messages(chat_id, seq, body) VALUES(?1, ?2, ?3)",
                        rusqlite::params![id, i as i64, body],
                    );
                }
                Err(e) => log::warn!("chat {id}: failed to serialize message {i}: {e}"),
            }
        }
        if let Err(e) = tx.commit() {
            log::warn!("replace chat {id}: commit failed: {e}");
        }
    }

    fn append(&self, id: &str, from_seq: usize, msgs: &[ChatMessageWire]) {
        let mut guard = self.conn.lock().unwrap();
        let tx = match guard.transaction() {
            Ok(t) => t,
            Err(e) => {
                log::warn!("append chat {id}: begin failed: {e}");
                return;
            }
        };
        for (offset, m) in msgs.iter().enumerate() {
            match serde_json::to_string(m) {
                Ok(body) => {
                    let _ = tx.execute(
                        "INSERT INTO messages(chat_id, seq, body) VALUES(?1, ?2, ?3)
                         ON CONFLICT(chat_id, seq) DO UPDATE SET body=excluded.body",
                        rusqlite::params![id, (from_seq + offset) as i64, body],
                    );
                }
                Err(e) => log::warn!("chat {id}: failed to serialize message: {e}"),
            }
        }
        if let Err(e) = tx.commit() {
            log::warn!("append chat {id}: commit failed: {e}");
        }
    }

    fn load_all(&self) -> Vec<PersistedChat> {
        let conn = self.conn.lock().unwrap();
        // Materialize chat metadata first so the statement is dropped before we query messages.
        let metas: Vec<(String, String, String, String, String, String)> = {
            let mut stmt = match conn
                .prepare("SELECT id, persona_slug, model_name, cwd, created_at, preset FROM chats")
            {
                Ok(s) => s,
                Err(e) => {
                    log::warn!("chats: prepare failed: {e}");
                    return Vec::new();
                }
            };
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                ))
            });
            match rows {
                Ok(it) => it.filter_map(Result::ok).collect(),
                Err(e) => {
                    log::warn!("chats: query failed: {e}");
                    return Vec::new();
                }
            }
        };

        let mut out = Vec::new();
        for (id, persona_slug, model_name, cwd, created_at, preset) in metas {
            let messages = self.load_messages(&conn, &id);
            let preset: SessionPreset = serde_json::from_str(&preset).unwrap_or_default();
            out.push(PersistedChat {
                id,
                persona_slug,
                model_name,
                cwd,
                created_at,
                preset,
                messages,
            });
        }
        out
    }

    fn delete(&self, id: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("DELETE FROM messages WHERE chat_id=?1", rusqlite::params![id]);
        if let Err(e) = conn.execute("DELETE FROM chats WHERE id=?1", rusqlite::params![id]) {
            log::warn!("failed to delete chat {id}: {e}");
        }
    }
}

impl SqliteStore {
    /// Ordered messages for one chat, malformed rows logged and skipped.
    fn load_messages(&self, conn: &Connection, id: &str) -> Vec<ChatMessageWire> {
        let mut stmt = match conn.prepare("SELECT seq, body FROM messages WHERE chat_id=?1 ORDER BY seq") {
            Ok(s) => s,
            Err(e) => {
                log::warn!("chat {id}: messages prepare failed: {e}");
                return Vec::new();
            }
        };
        let rows = match stmt.query_map(rusqlite::params![id], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        }) {
            Ok(it) => it,
            Err(e) => {
                log::warn!("chat {id}: messages query failed: {e}");
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for row in rows {
            match row {
                Ok((seq, body)) => match serde_json::from_str::<ChatMessageWire>(&body) {
                    Ok(m) => out.push(m),
                    Err(e) => log::warn!("chat {id}: failed to parse message seq {seq}: {e}"),
                },
                Err(e) => log::warn!("chat {id}: message row error: {e}"),
            }
        }
        out
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

    fn chat_with(id: &str, msgs: usize) -> PersistedChat {
        let messages: Vec<_> = (0..msgs)
            .map(|i| serde_json::json!({"role": "user", "content": format!("m{i}")}))
            .collect();
        serde_json::from_value(serde_json::json!({
            "id": id, "persona_slug": "p", "model_name": "m", "cwd": "/", "created_at": "t",
            "messages": messages,
        }))
        .unwrap()
    }

    #[test]
    fn chats_replace_round_trip() {
        let (_dir, s) = temp_store();
        assert!(ChatStore::load_all(&s).is_empty());
        ChatStore::replace(&s, "c1", &chat_with("c1", 2));
        let all = ChatStore::load_all(&s);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].messages.len(), 2);
        // replace with fewer messages (the idle-reset case) must not leave stale rows
        ChatStore::replace(&s, "c1", &chat_with("c1", 1));
        let all = ChatStore::load_all(&s);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].messages.len(), 1);
        ChatStore::delete(&s, "c1");
        assert!(ChatStore::load_all(&s).is_empty());
    }

    #[test]
    fn chats_append_extends_transcript() {
        let (_dir, s) = temp_store();
        let base = chat_with("c1", 2);
        ChatStore::replace(&s, "c1", &base);
        // append two more at seq 2,3
        let more = chat_with("c1", 4).messages[2..].to_vec();
        ChatStore::append(&s, "c1", 2, &more);
        let all = ChatStore::load_all(&s);
        assert_eq!(all[0].messages.len(), 4);
        // append is idempotent per seq (re-appending the same seqs doesn't duplicate)
        ChatStore::append(&s, "c1", 2, &more);
        assert_eq!(ChatStore::load_all(&s)[0].messages.len(), 4);
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
