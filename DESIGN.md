# metalcraft-agent-r2 — DESIGN

A fork of `metalcraft-agent` that replaces its loose-JSON-files persistence with a **SQLite-backed
`Store` trait + Litestream replication to R2**, so it can run on an ephemeral-disk Cloudflare
Container (see `metalcraft-do-cluster`) with RPO ≈ 1s — while staying a drop-in for k3 self-host.

Architecture choice (settled): **"A, built with B's seam"** — one agent process + one SQLite DB per
user; chats are independently addressable so a per-chat streaming `SessionDO` can be added later
(architecture "B") with **zero storage migration**. No decomposition of the turn loop (that's "C").

Rust edition 2024, rust-version 1.91 (ecosystem convention).

---

## 0. Non-negotiables / goals
1. **Coexist, don't hard-fork behavior.** Gate the backend on `METALCRAFT_STORE=files|sqlite`
   (default `files` = today's behavior). k3 and Tauri self-host keep working; R2 deployments set
   `sqlite`. This makes the store refactor a net upgrade everywhere, not a divergent branch.
2. **Fix the three latent bugs the audit found** (they bite k3 today too):
   - non-atomic chat/flow writes → transactional, append-only messages.
   - `keys.json` plaintext → encrypted at rest.
   - empty `/data` = silent total data loss → explicit "fresh vs lost" is a non-issue once Litestream
     restores; plus a schema `meta` row makes "initialized" detectable.
3. **Litestream-ready:** DB in WAL mode at a fixed path; replication is a container sidecar concern,
   not in-process (the agent just needs correct pragmas + a stable file).
4. **Chat seam:** a clean `/api/v1/chats/{id}/turn` (SSE) contract so architecture B can front a
   single chat with a hibernatable-WebSocket DO without touching storage.
5. **Uploads leave the DB:** `uploads/` blobs go to R2 (object storage), never into SQLite.

---

## 1. The `Store` trait (ports)
Introduce one storage port, composed of the sub-stores the audit found. Everything that currently
does `std::fs` against `paths::*` moves behind this. Two implementations: `FilesStore` (wraps
today's code, for `files` mode + as the migration *source*) and `SqliteStore`.

```rust
// crates/store/src/lib.rs  (new)
#[async_trait]
pub trait Store: Send + Sync {
    fn chats(&self) -> &dyn ChatStore;
    fn keys(&self) -> &dyn KeyStore;
    fn flow_runs(&self) -> &dyn FlowRunStore;
    fn scheduled(&self) -> &dyn ScheduledStore;
    fn gateway(&self) -> &dyn GatewayStore;
    fn packs(&self) -> &dyn PackStore;
    fn overrides(&self) -> &dyn OverrideStore;   // persona/skill/flow/api_tool user overrides
    async fn migrate(&self) -> Result<()>;        // schema init / version bump
}

#[async_trait]
pub trait ChatStore {
    async fn list(&self) -> Result<Vec<ChatSummary>>;
    async fn load(&self, id: &str) -> Result<Option<PersistedChat>>;
    async fn create(&self, chat: &PersistedChat) -> Result<()>;
    async fn append_message(&self, chat_id: &str, msg: &ChatMessage) -> Result<()>; // <-- append, not rewrite
    async fn update_meta(&self, chat_id: &str, persona: &str, model: &str, cwd: &str) -> Result<()>;
    async fn delete(&self, id: &str) -> Result<()>;
}
// KeyStore { get/set/list/delete }  — SqliteStore encrypts value at rest (AES-GCM)
// FlowRunStore { save/load/list/delete }
// ScheduledStore { list/upsert/remove/due_before(ts) }
// GatewayStore { channels CRUD; dedup_seen(id)->bool + record(id, ttl) }
// PackStore { list/get/set install+enable state }
// OverrideStore { get/put user overrides for personas/skills/flows/api_tools }
```

The existing serde structs (`PersistedChat`, `FlowRun`/`SavedFlow`, scheduled task, gateway channel,
etc.) are reused verbatim as the trait's data types — the SQLite impl serializes the leaf structs it
doesn't want to normalize (e.g. store a `ChatMessage` as a JSON column) and normalizes the parts that
need querying (chat id, updated_at, chat_id FK).

Call sites (from the audit: `workshop_api.rs` persist_chat ×7, `flow_runs.rs`, `key_store.rs`,
`scheduled_tasks.rs`, `gateway_channels.rs`, `inbound_dedup.rs`, `integration_packs.rs`) change from
direct `fs` calls to `store.chats().append_message(...)` etc. `paths.rs` stays for `files` mode and
for locating the SQLite file + uploads root.

---

## 2. SQLite schema
One DB per user at `${METALCRAFT_DATA_DIR}/agent.db`, WAL mode. Key design point: **messages are their
own append-only table**, so a chat turn is a single-row INSERT, not a full-history rewrite — this
removes the write-amplification *and* the torn-file risk in one move.

```sql
PRAGMA journal_mode=WAL;         -- required for Litestream
PRAGMA busy_timeout=5000;
PRAGMA synchronous=NORMAL;       -- WAL + NORMAL is durable enough; Litestream ships the WAL

CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT);          -- schema_version, seeded markers, sub

CREATE TABLE chats(
  id TEXT PRIMARY KEY, persona TEXT, model TEXT, cwd TEXT,
  created_at INTEGER, updated_at INTEGER
);
CREATE TABLE messages(                                  -- append-only; the hot path
  chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
  seq INTEGER NOT NULL,                                 -- per-chat ordinal
  role TEXT, body TEXT,                                 -- body = serialized ChatMessage (JSON)
  created_at INTEGER,
  PRIMARY KEY(chat_id, seq)
);

CREATE TABLE keys(name TEXT PRIMARY KEY, value_enc BLOB, nonce BLOB, updated_at INTEGER); -- AES-GCM
CREATE TABLE flow_runs(id TEXT PRIMARY KEY, state TEXT, updated_at INTEGER);              -- state=JSON FlowRun
CREATE TABLE scheduled_tasks(id TEXT PRIMARY KEY, due_at INTEGER, spec TEXT);
CREATE TABLE gateway_channels(id TEXT PRIMARY KEY, config TEXT);
CREATE TABLE inbound_dedup(id TEXT PRIMARY KEY, seen_at INTEGER);                          -- TTL-swept
CREATE TABLE integration_packs(pack TEXT PRIMARY KEY, state TEXT);
CREATE TABLE overrides(kind TEXT, name TEXT, body TEXT, PRIMARY KEY(kind,name));           -- persona/skill/flow/api_tool
```

Seeded defaults (personas/skills/flows/api_tools) stay embedded in the binary and are applied at boot
as today; only *user-modified* copies land in `overrides`. This keeps the DB small and the seed logic
(`seed::ensure_defaults`) unchanged.

---

## 3. Crate & concurrency
- **`sqlx` with the `sqlite` driver** (async, native to the tokio/axum agent, compile-time-checked
  queries, built-in migrations). Single write connection + a small read pool; WAL allows concurrent
  reads during a write. (Alternative: `rusqlite` + `deadpool` behind `spawn_blocking` — more manual.)
- All mutations are transactions → atomic by construction (fixes the non-atomic writes).
- `keys` encryption: AES-GCM with a key from env (`METALCRAFT_STORE_KEY`), mirroring the ecosystem's
  existing AES-GCM vault pattern (metalcraft-email / gateway). Never store plaintext secrets.

---

## 4. Uploads → R2
Replace the local `uploads/` tree with object storage:
- `UploadStore` writes/reads via S3 API (reuse `tools/spaces.rs` SigV4 signer, or `object_store`
  which is already transitively in the tree). Bucket/prefix from env (`R2_*` / `METALCRAFT_UPLOAD_*`).
- Serve downloads via presigned GET or a proxied Range endpoint (the drive subapp already has this
  pattern). Uploads never touch the SQLite DB or the ephemeral disk.

---

## 5. Litestream wiring (lives in metalcraft-do-cluster, documented here)
The agent only guarantees: WAL mode + DB at `${METALCRAFT_DATA_DIR}/agent.db`. The **container**
(metalcraft-do-cluster) runs Litestream as the process supervisor:
```yaml
# litestream.yml
dbs:
  - path: /data/agent.db
    replicas:
      - type: s3
        endpoint: ${R2_ENDPOINT}
        bucket:   ${R2_BUCKET}
        path:     agents/${SUB}/agent.db
```
```sh
# entrypoint (replaces the tar entrypoint once agent-r2 ships)
litestream restore -if-replica-exists -o /data/agent.db "s3://${R2_BUCKET}/agents/${SUB}/agent.db"
exec litestream replicate -exec "metalcraft-agent" -config /etc/litestream.yml
```
For k3/self-host, Litestream is optional — the DB on a PVC works unchanged.

---

## 6. Migration from existing users (files → sqlite)
A one-shot importer (`metalcraft-agent-r2 migrate-store` subcommand or auto-run when `sqlite` mode
finds a populated legacy tree + empty DB):
1. Read the old JSON tree via `FilesStore` (the audit's file map is the exhaustive source).
2. Write into `SqliteStore` in one transaction per store.
3. Stamp `meta.schema_version` + `meta.migrated_from_files=1`.
Idempotent; safe to re-run. Test on a scratch copy of a real `/data` (ecosystem convention: migrations
verified on a scratch DB before rollout).

---

## 7. Chat seam (makes architecture B a drop-in)
Guarantee a per-chat turn contract so a future `SessionDO` can own one chat's WebSocket:
- `POST /api/v1/chats/{id}/turn` → SSE stream of the turn (this likely already exists inside
  `workshop_api.rs`; formalize + document it, ensure it's addressable per chat id and stateless w.r.t.
  connection — all durable writes go through `ChatStore.append_message`).
- No shared in-memory chat state that a second front-end couldn't reconstruct from the DB.
This is a *contract*, not new subsystems — B later puts a TS DO in front of this endpoint.

---

## 8. Milestones
- **S1 — `Store` trait + `FilesStore`.** Extract the port; wrap existing code with zero behavior
  change. Because persistence is a mix of path-resolving free fns, `&Path` methods, in-module locks,
  and inline handler writes, S1 is done as a **vertical slice per area** rather than one big-bang:
  - **S1a — chats (DONE).** `src/store/mod.rs`: `Store` + `ChatStore` traits + `FilesStore`; `store()`
    global selected by `METALCRAFT_STORE` (`sqlite` warns + falls back in S1). Routed `persist_chat`,
    `remove_chat_file`, `load_persisted_chats`, `read_persisted_chats` through it. `FilesChats` bodies
    are the originals verbatim → zero behavior change. `cargo check` green.
    - Deferred within chats: `workshop_api.rs:~3541` still calls `chat_file_path` for an mtime check
      (a files-only detail); migrate when SQLite lands.
  - **S1b — flow runs + collection stores (DONE).** Added `FlowRunStore` + `ScheduledStore`/
    `KeysStore`/`GatewayStore`/`PackStore` sub-traits, each a `FilesStore` facade over today's fns
    (preserving in-module locks + tmp+rename atomicity). Routed all call sites for: flow runs (10),
    scheduled tasks (6 fns), key lookups + the `KeyStore` load-mutate-save vault, gateway *instance*
    ops (9), and pack *enabled-state* (`is_enabled`/`set_enabled`/`load_state`). Deliberately left as
    free functions: pure helpers (`mask`, `normalize_number`), file-layering resolvers
    (`resolve_file`/`list_files_layered`), and seeded read-only content (channel types, installed pack
    dirs). Full suite 24/24 targets + 134 lib tests green.
  - Baseline note: `tests/phase5_6_7_test.rs` is **pre-existing broken** upstream (11× E0533, stale vs
    the current `AgentUpdate` API) — not caused by this work; lib+bins compile clean.
- **S2 — `SqliteStore` behind `METALCRAFT_STORE=sqlite` (DONE, chose `rusqlite`+bundled over sqlx —
  the `Store` traits are sync).** `<data>/agent.db`, WAL + `busy_timeout` + NORMAL sync. Backs the
  **blob-per-entity hot-write stores** (chats, flow runs) as `id → JSON body` tables, upsert on save;
  round-trip + WAL tests. Currently a **documented hybrid**: the logic-bearing collection stores
  (scheduled/keys/gateway/packs) are delegated to the files backend because their logic is intertwined
  with file I/O — backing them by reimplementing would risk divergence.
- **S2b — collection stores → SQLite via a shared document seam (DONE).** Added a low-level
  `DocStore` (get/put a named JSON document) on the `Store` port. `FilesDocs` maps
  `name → <data>/<name>.json` (atomic tmp+fsync+rename); `SqliteStore` gained a `docs(name, body)`
  table. Refactored the private load/save primitives in `scheduled_tasks`/`gateway_channels`/
  `integration_packs` and the keys vault (`KeyStore::load_current`/`from_json_str`,
  `lookup`/`lookup_scoped`) to persist through `store().docs()`, keeping parsing/migration/locking.
  Because the modules call the *global* store's docs, `sqlite` mode now puts **all** state in one
  `agent.db` (single Litestream target) — the hybrid is gone; files mode is unchanged. sqlite tests
  4/4, full suite green. Caveat: switching an existing files agent to sqlite starts empty (import = S5).
- **S3a — keys at rest (DONE).** AES-256-GCM seal/open of the keys vault document when
  `METALCRAFT_STORE_KEY` is set (random nonce, `enc:v1:` marker); passthrough + legacy plaintext load
  otherwise. Wired into `KeyStore::load_current` (open) + the save path (seal). Crypto + regression
  tests green. `aes-gcm 0.10`.
- **S3b — append-only chat messages (IN PROGRESS).** Normalize chats into `chats(meta)` +
  append-only `messages(chat_id, seq, body)` so a turn writes only new rows (Litestream ships a small
  delta instead of the whole transcript). Needs a `ChatStore::append`/`upsert_meta` split, a
  per-session persisted-message cursor in `persist_chat`, and `PersistedChat`'s message type reachable.
  Gated on verifying messages are strictly append-only across persists (no in-place mutation/truncation
  of already-persisted messages) — under analysis.
- **S4 — uploads → R2.** Remove `uploads/` from disk.
- **S5 — migration importer + Litestream.** `migrate-store`; WAL/path guarantees; scratch-DB test.
- **S6 — chat seam.** Formalize `/api/v1/chats/{id}/turn` SSE contract + docs for B.

Then `metalcraft-do-cluster` swaps its container entrypoint from tar (Option A/M0) to
`litestream replicate -exec`, and points its image at `metalcraft-agent-r2`.

---

## 9. Open questions
- `sqlx` vs `rusqlite`: confirm the agent's async story tolerates sqlx's connection model under the
  turn loop's blocking-ish tool calls.
- Message body: fully JSON blob vs partly-normalized (tool calls, attachments) — start blob, normalize
  only if query needs arise.
- Litestream restore time for large chat histories → feeds the `sleepAfter` / cold-start budget in
  the cost model.
- Do personas/skills that are *seeded then version-upgraded* need a hash column to detect user edits vs
  stale seeds? (Currently force-upgraded on version bump.)
