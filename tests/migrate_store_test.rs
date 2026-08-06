//! End-to-end test for `store::migrate_files_to_sqlite`: write legacy files under a temp data dir,
//! migrate, and verify the rows landed in agent.db.
//!
//! This is its own test binary so `paths::data_dir()` (memoized once per process) resolves to our
//! temp dir — we set `METALCRAFT_DATA_DIR` before any code touches it.

#[test]
fn migrate_files_to_sqlite_copies_state() {
    let tmp = std::env::temp_dir().join(format!("mc-migrate-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(tmp.join("chats")).unwrap();
    std::fs::create_dir_all(tmp.join("runs")).unwrap();
    // Set the data dir before the lib resolves it (fresh process → first resolution wins).
    unsafe {
        std::env::set_var("METALCRAFT_DATA_DIR", &tmp);
    }

    // Legacy files: two docs, one chat (2 messages), one flow run.
    std::fs::write(
        tmp.join("keys.json"),
        r#"{"version":2,"global":{"FOO":"bar"},"channels":{}}"#,
    )
    .unwrap();
    std::fs::write(tmp.join("scheduled_tasks.json"), "[]").unwrap();
    std::fs::write(
        tmp.join("chats/c1.json"),
        r#"{"id":"c1","persona_slug":"p","model_name":"m","cwd":"/","created_at":"t",
            "messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"yo"}]}"#,
    )
    .unwrap();
    std::fs::write(
        tmp.join("runs/r1.json"),
        r#"{"id":"r1","flow_id":"f","status":"paused","current_node_id":"n","variables":{},
            "pause":null,"persona":"p","model":"m","cwd":"/","steps":[],"flow":null,
            "created_at":"t","updated_at":"t"}"#,
    )
    .unwrap();

    let db = metalcraft_agent::store::default_sqlite_db_path();
    assert_eq!(db, tmp.join("agent.db"));
    let stats = metalcraft_agent::store::migrate_files_to_sqlite(db.clone()).unwrap();
    assert_eq!(stats.docs, 2, "keys + scheduled_tasks");
    assert_eq!(stats.chats, 1);
    assert_eq!(stats.flow_runs, 1);

    // Verify the rows actually landed in agent.db.
    let conn = rusqlite::Connection::open(&db).unwrap();
    let docs: i64 = conn
        .query_row("SELECT count(*) FROM docs", [], |r| r.get(0))
        .unwrap();
    let chats: i64 = conn
        .query_row("SELECT count(*) FROM chats", [], |r| r.get(0))
        .unwrap();
    let msgs: i64 = conn
        .query_row("SELECT count(*) FROM messages WHERE chat_id='c1'", [], |r| r.get(0))
        .unwrap();
    let runs: i64 = conn
        .query_row("SELECT count(*) FROM flow_runs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(docs, 2);
    assert_eq!(chats, 1);
    assert_eq!(msgs, 2, "the chat's two messages were normalized into rows");
    assert_eq!(runs, 1);

    // The keys doc was copied verbatim (plaintext here — no METALCRAFT_STORE_KEY set).
    let keys_body: String = conn
        .query_row("SELECT body FROM docs WHERE name='keys'", [], |r| r.get(0))
        .unwrap();
    assert!(keys_body.contains("\"FOO\":\"bar\""));

    // Idempotent: a second run doesn't duplicate.
    metalcraft_agent::store::migrate_files_to_sqlite(db.clone()).unwrap();
    let chats2: i64 = conn
        .query_row("SELECT count(*) FROM chats", [], |r| r.get(0))
        .unwrap();
    assert_eq!(chats2, 1);

    let _ = std::fs::remove_dir_all(&tmp);
}
