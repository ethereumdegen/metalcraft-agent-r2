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

mod sqlite;

use std::collections::HashMap;
use std::sync::OnceLock;

use chrono::{DateTime, Utc};

use crate::flow_runs::FlowRun;
use crate::gateway_channels::ChannelInstance;
use crate::integration_packs::PackState;
use crate::key_store::KeyStore;
use crate::scheduled_tasks::{NewTask, ScheduledTask, TaskStatus};
use crate::workshop_api::{ChatMessageWire, PersistedChat};

/// The storage port. One process-wide instance, obtained via [`store`].
///
/// Only functions that read/write **persistent state** live behind this port. Pure helpers
/// (`key_store::mask`, `gateway_channels::normalize_number`, …), file-layering resolvers
/// (`integration_packs::resolve_file`/`list_files_layered`), and seeded read-only *content*
/// (channel types, installed pack directories, personas/skills) stay as free functions — they are
/// not per-user state and are regenerated from embedded seeds, so they don't belong in the DB.
pub(crate) trait Store: Send + Sync {
    fn chats(&self) -> &dyn ChatStore;
    fn flow_runs(&self) -> &dyn FlowRunStore;
    fn scheduled(&self) -> &dyn ScheduledStore;
    fn keys(&self) -> &dyn KeysStore;
    fn gateway(&self) -> &dyn GatewayStore;
    fn packs(&self) -> &dyn PackStore;
    fn docs(&self) -> &dyn DocStore;
}

/// Low-level named-JSON-document persistence. The logic-bearing collection modules
/// (`scheduled_tasks`, `gateway_channels`, `integration_packs`, the keys vault) keep their parsing,
/// migration and locking, and persist the raw document through this seam — so their behavior is
/// backend-agnostic. Files backend maps `name -> <data>/<name>.json` (identical to the historical
/// files); sqlite backend maps to a `docs(name, body)` table in `agent.db`.
pub(crate) trait DocStore: Send + Sync {
    /// The document body, or `None` if it has never been written.
    fn get(&self, name: &str) -> Option<String>;
    /// Create or replace the document, durably (atomic rename / transactional upsert).
    fn put(&self, name: &str, body: &str) -> std::io::Result<()>;
}

/// Persistence for chat transcripts. Writes are split so the common per-turn path only *appends*
/// new messages (a small Litestream delta) instead of rewriting the whole transcript:
/// - [`replace`](Self::replace): full write — creating a chat, or resetting one whose message count
///   regressed (the gateway idle-reset drops history). Callers use it when `messages.len() <
///   persisted_count` or on first persist.
/// - [`append`](Self::append): the hot path — add messages at `seq >= from_seq`.
///
/// The append-only invariant (messages never mutate/truncate in place except the idle-reset, which
/// the `replace` fallback covers) was verified against the agent's turn loop.
pub(crate) trait ChatStore: Send + Sync {
    /// Full write: replace metadata + all messages for `id`.
    fn replace(&self, id: &str, chat: &PersistedChat);
    /// Append `msgs` at sequence numbers `from_seq, from_seq+1, …` (idempotent per seq).
    fn append(&self, id: &str, from_seq: usize, msgs: &[ChatMessageWire]);
    /// Load every persisted transcript. Malformed entries are logged and skipped.
    fn load_all(&self) -> Vec<PersistedChat>;
    /// Delete the transcript for `id` if present.
    fn delete(&self, id: &str);
}

/// Persistence for paused/terminal flow runs (`<data>/runs/<id>.json` in the files backend).
pub(crate) trait FlowRunStore: Send + Sync {
    fn save(&self, run: &FlowRun) -> std::io::Result<()>;
    fn load(&self, id: &str) -> Option<FlowRun>;
    fn list(&self) -> Vec<FlowRun>;
}

/// Persistence for scheduled follow-up tasks (`<data>/scheduled_tasks.json`).
pub(crate) trait ScheduledStore: Send + Sync {
    fn list(&self) -> Vec<ScheduledTask>;
    fn add(&self, new: NewTask) -> Result<ScheduledTask, String>;
    fn cancel(&self, id: &str) -> Result<bool, String>;
    fn requeue(&self, id: &str, run_at: DateTime<Utc>);
    fn mark(&self, id: &str, status: TaskStatus);
    fn claim_due(&self, now: DateTime<Utc>) -> Vec<ScheduledTask>;
}

/// The scoped key/secret vault (`<data>/keys.json`). `load`/`save` round-trip the whole vault;
/// `lookup`/`lookup_scoped` resolve a single secret (stored value overlaid with env).
pub(crate) trait KeysStore: Send + Sync {
    fn load(&self) -> KeyStore;
    fn save(&self, ks: &KeyStore) -> std::io::Result<()>;
    fn lookup(&self, name: &str) -> Option<String>;
    fn lookup_scoped(&self, channel_id: Option<&str>, name: &str) -> Option<String>;
}

/// Gateway channel *instances* (user config, `<data>/gateway_channels.json`). Channel *types*
/// are seeded content and stay as free functions in `gateway_channels`.
pub(crate) trait GatewayStore: Send + Sync {
    fn load_instances(&self) -> Vec<ChannelInstance>;
    fn get_instance(&self, id: &str) -> Option<ChannelInstance>;
    fn enabled_instances(&self) -> Vec<ChannelInstance>;
    fn create_instance(&self, type_id: &str, name: &str, settings: HashMap<String, String>) -> Result<ChannelInstance, String>;
    fn update_instance(&self, id: &str, name: &str, enabled: bool, settings: HashMap<String, String>) -> Result<ChannelInstance, String>;
    fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), String>;
    fn delete_instance(&self, id: &str) -> Result<bool, String>;
    fn resolve_by_setting(&self, key: &str, value: &str) -> Option<ChannelInstance>;
    fn resolve_by_inbound_to(&self, to_number: &str) -> Option<ChannelInstance>;
}

/// Integration-pack *enabled state* (`<data>/integration_packs.json`). Installed pack *content*
/// (directories of personas/skills/tools) and the layered file resolvers stay as free functions.
pub(crate) trait PackStore: Send + Sync {
    fn is_enabled(&self, id: &str) -> bool;
    fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), String>;
    fn load_state(&self) -> HashMap<String, PackState>;
}

/// Process-wide store, initialized once from `METALCRAFT_STORE`.
pub(crate) fn store() -> &'static dyn Store {
    static STORE: OnceLock<Box<dyn Store>> = OnceLock::new();
    STORE
        .get_or_init(|| match std::env::var("METALCRAFT_STORE").as_deref() {
            Ok("sqlite") => match sqlite::SqliteStore::open_default() {
                Ok(s) => {
                    log::info!("storage backend: sqlite ({})", sqlite::default_db_path().display());
                    Box::new(s)
                }
                Err(e) => {
                    log::error!(
                        "METALCRAFT_STORE=sqlite: could not open the database ({e}); \
                         falling back to the files backend"
                    );
                    Box::new(FilesStore)
                }
            },
            _ => Box::new(FilesStore),
        })
        .as_ref()
}

// ---- files backend -------------------------------------------------------------------------

/// The shared files-backed instance, reused by the sqlite backend for the stores it does not
/// (yet) own (scheduled/keys/gateway/packs).
pub(super) fn files_backend() -> &'static FilesStore {
    static F: FilesStore = FilesStore;
    &F
}

pub(super) struct FilesStore;

impl Store for FilesStore {
    fn chats(&self) -> &dyn ChatStore {
        static CHATS: FilesChats = FilesChats;
        &CHATS
    }
    fn flow_runs(&self) -> &dyn FlowRunStore {
        static RUNS: FilesFlowRuns = FilesFlowRuns;
        &RUNS
    }
    fn scheduled(&self) -> &dyn ScheduledStore {
        static SCHED: FilesScheduled = FilesScheduled;
        &SCHED
    }
    fn keys(&self) -> &dyn KeysStore {
        static KEYS: FilesKeys = FilesKeys;
        &KEYS
    }
    fn gateway(&self) -> &dyn GatewayStore {
        static GW: FilesGateway = FilesGateway;
        &GW
    }
    fn packs(&self) -> &dyn PackStore {
        static PACKS: FilesPacks = FilesPacks;
        &PACKS
    }
    fn docs(&self) -> &dyn DocStore {
        static DOCS: FilesDocs = FilesDocs;
        &DOCS
    }
}

/// Files backend for named JSON documents: `name -> <data>/<name>.json`, atomic (tmp + fsync +
/// rename). The fsync matches the strongest historical behavior (integration_packs) and is safe to
/// apply to the others.
struct FilesDocs;

impl DocStore for FilesDocs {
    fn get(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(crate::paths::data_dir().join(format!("{name}.json"))).ok()
    }
    fn put(&self, name: &str, body: &str) -> std::io::Result<()> {
        let path = crate::paths::data_dir().join(format!("{name}.json"));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut f, body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)
    }
}

struct FilesChats;

impl FilesChats {
    fn write(&self, id: &str, chat: &PersistedChat) {
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
    fn read(&self, id: &str) -> Option<PersistedChat> {
        let path = crate::paths::chats_dir().join(format!("{id}.json"));
        let content = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&content).ok()
    }
}

impl ChatStore for FilesChats {
    fn replace(&self, id: &str, chat: &PersistedChat) {
        self.write(id, chat);
    }

    // Files backend has no WAL-delta benefit; keep it correct by read-truncate-extend-rewrite.
    fn append(&self, id: &str, from_seq: usize, msgs: &[ChatMessageWire]) {
        let Some(mut pc) = self.read(id) else {
            log::warn!("append to missing chat {id}; skipping");
            return;
        };
        pc.messages.truncate(from_seq);
        pc.messages.extend(msgs.iter().cloned());
        self.write(id, &pc);
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

/// Files backend for flow runs — delegates to the existing `crate::flow_runs` free functions
/// with `runs_dir()` resolved internally (every call site used `paths::runs_dir()`).
struct FilesFlowRuns;

impl FlowRunStore for FilesFlowRuns {
    fn save(&self, run: &FlowRun) -> std::io::Result<()> {
        crate::flow_runs::save_run(&crate::paths::runs_dir(), run)
    }
    fn load(&self, id: &str) -> Option<FlowRun> {
        crate::flow_runs::load_run(&crate::paths::runs_dir(), id)
    }
    fn list(&self) -> Vec<FlowRun> {
        crate::flow_runs::list_runs(&crate::paths::runs_dir())
    }
}

/// Files backend for scheduled tasks — delegates to `crate::scheduled_tasks` (which keeps its own
/// process-wide advisory mutex and resolves `scheduled_tasks_file()` internally).
struct FilesScheduled;

impl ScheduledStore for FilesScheduled {
    fn list(&self) -> Vec<ScheduledTask> {
        crate::scheduled_tasks::list()
    }
    fn add(&self, new: NewTask) -> Result<ScheduledTask, String> {
        crate::scheduled_tasks::add(new)
    }
    fn cancel(&self, id: &str) -> Result<bool, String> {
        crate::scheduled_tasks::cancel(id)
    }
    fn requeue(&self, id: &str, run_at: DateTime<Utc>) {
        crate::scheduled_tasks::requeue(id, run_at)
    }
    fn mark(&self, id: &str, status: TaskStatus) {
        crate::scheduled_tasks::mark(id, status)
    }
    fn claim_due(&self, now: DateTime<Utc>) -> Vec<ScheduledTask> {
        crate::scheduled_tasks::claim_due(now)
    }
}

/// Files backend for the key vault — `keys_file()` resolved internally.
struct FilesKeys;

impl KeysStore for FilesKeys {
    fn load(&self) -> KeyStore {
        // Loads via the active DocStore (files or sqlite), applying legacy migration.
        KeyStore::load_current()
    }
    fn save(&self, ks: &KeyStore) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(ks).map_err(std::io::Error::other)?;
        // Encrypt at rest when METALCRAFT_STORE_KEY is set (passthrough otherwise).
        store().docs().put("keys", &crate::key_store::seal(&json))
    }
    fn lookup(&self, name: &str) -> Option<String> {
        crate::key_store::lookup(name)
    }
    fn lookup_scoped(&self, channel_id: Option<&str>, name: &str) -> Option<String> {
        crate::key_store::lookup_scoped(channel_id, name)
    }
}

/// Files backend for gateway channel instances — delegates to `crate::gateway_channels`.
struct FilesGateway;

impl GatewayStore for FilesGateway {
    fn load_instances(&self) -> Vec<ChannelInstance> {
        crate::gateway_channels::load_instances()
    }
    fn get_instance(&self, id: &str) -> Option<ChannelInstance> {
        crate::gateway_channels::get_instance(id)
    }
    fn enabled_instances(&self) -> Vec<ChannelInstance> {
        crate::gateway_channels::enabled_instances()
    }
    fn create_instance(&self, type_id: &str, name: &str, settings: HashMap<String, String>) -> Result<ChannelInstance, String> {
        crate::gateway_channels::create_instance(type_id, name, settings)
    }
    fn update_instance(&self, id: &str, name: &str, enabled: bool, settings: HashMap<String, String>) -> Result<ChannelInstance, String> {
        crate::gateway_channels::update_instance(id, name, enabled, settings)
    }
    fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), String> {
        crate::gateway_channels::set_enabled(id, enabled)
    }
    fn delete_instance(&self, id: &str) -> Result<bool, String> {
        crate::gateway_channels::delete_instance(id)
    }
    fn resolve_by_setting(&self, key: &str, value: &str) -> Option<ChannelInstance> {
        crate::gateway_channels::resolve_by_setting(key, value)
    }
    fn resolve_by_inbound_to(&self, to_number: &str) -> Option<ChannelInstance> {
        crate::gateway_channels::resolve_by_inbound_to(to_number)
    }
}

/// Files backend for integration-pack enabled state — delegates to `crate::integration_packs`
/// (which keeps its cross-process `.lock` file around the state map).
struct FilesPacks;

impl PackStore for FilesPacks {
    fn is_enabled(&self, id: &str) -> bool {
        crate::integration_packs::is_enabled(id)
    }
    fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), String> {
        crate::integration_packs::set_enabled(id, enabled)
    }
    fn load_state(&self) -> HashMap<String, PackState> {
        crate::integration_packs::load_state()
    }
}
