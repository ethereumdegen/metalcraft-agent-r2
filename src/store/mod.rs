//! Storage port for metalcraft-agent-r2.
//!
//! S1 (this): introduce the `Store` trait and a files-backed implementation, and route the
//! **chats** hot path through it — zero behavior change vs the original `std::fs` code. The
//! backend is selected by `METALCRAFT_STORE` (`files` | `sqlite`); only `files` exists yet, so
//! `sqlite` transparently falls back with a warning. Later steps (see DESIGN.md S2–S6) grow the
//! trait to cover keys/flow-runs/scheduled/gateway/packs and add the SQLite + Litestream backend.
//!
//! The trait deals in the *serialized* DTOs (e.g. `PersistedChat`), never the in-memory session
//! types — rehydration into `ChatSession`/`AgentState` stays in `workshop_api`.

use std::sync::OnceLock;

use crate::workshop_api::PersistedChat;

/// The storage port. One process-wide instance, obtained via [`store`].
pub(crate) trait Store: Send + Sync {
    fn chats(&self) -> &dyn ChatStore;
}

/// Persistence for chat transcripts (`<data>/chats/<id>.json` in the files backend).
pub(crate) trait ChatStore: Send + Sync {
    /// Write the full transcript for `id`. Overwrites any existing copy.
    fn save(&self, id: &str, chat: &PersistedChat);
    /// Load every persisted transcript. Malformed entries are logged and skipped.
    fn load_all(&self) -> Vec<PersistedChat>;
    /// Delete the transcript for `id` if present.
    fn delete(&self, id: &str);
}

/// Process-wide store, initialized once from `METALCRAFT_STORE`.
pub(crate) fn store() -> &'static dyn Store {
    static STORE: OnceLock<Box<dyn Store>> = OnceLock::new();
    STORE
        .get_or_init(|| match std::env::var("METALCRAFT_STORE").as_deref() {
            Ok("sqlite") => {
                log::warn!(
                    "METALCRAFT_STORE=sqlite is not implemented yet (S2); using the files backend"
                );
                Box::new(FilesStore)
            }
            _ => Box::new(FilesStore),
        })
        .as_ref()
}

// ---- files backend -------------------------------------------------------------------------

struct FilesStore;

impl Store for FilesStore {
    fn chats(&self) -> &dyn ChatStore {
        static CHATS: FilesChats = FilesChats;
        &CHATS
    }
}

struct FilesChats;

impl ChatStore for FilesChats {
    fn save(&self, id: &str, chat: &PersistedChat) {
        let path = crate::paths::chats_dir().join(format!("{id}.json"));
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(chat) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&path, json) {
                    log::warn!("failed to persist chat {id}: {e}");
                }
            }
            Err(e) => log::warn!("failed to serialize chat {id}: {e}"),
        }
    }

    fn load_all(&self) -> Vec<PersistedChat> {
        let dir = crate::paths::chats_dir();
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            match std::fs::read_to_string(&path).map(|c| serde_json::from_str::<PersistedChat>(&c)) {
                Ok(Ok(pc)) => out.push(pc),
                Ok(Err(e)) => log::warn!("failed to parse chat file {}: {e}", path.display()),
                Err(e) => log::warn!("failed to read chat file {}: {e}", path.display()),
            }
        }
        out
    }

    fn delete(&self, id: &str) {
        let path = crate::paths::chats_dir().join(format!("{id}.json"));
        if path.exists() {
            if let Err(e) = std::fs::remove_file(&path) {
                log::warn!("failed to delete chat file {}: {e}", path.display());
            }
        }
    }
}
