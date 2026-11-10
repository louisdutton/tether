mod support;
use agent_client_protocol::v1::schema::SessionConfigOptionValue;
use std::{path::Path, sync::Arc};
use support::Fixture;
use tether::{
    manager::{Error, Manager},
    model::{AdoptSession, SessionKind},
    providers::{Provider, acp::AcpProvider},
};

fn seed(f: &Fixture, agent: &str, id: &str, cwd: &Path) {
    let directory = f.directory.path().join(agent);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join(format!("{id}.json")),
        serde_json::to_vec(&serde_json::json!({
            "sessionId":id,"cwd":cwd,"title":"Same title","mcpServers":[]
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(directory.join(format!("{id}.history")), "").unwrap();
}

#[tokio::test]
async fn catalog_merges_all_bindings_without_title_or_native_id_collisions() {
    let f = Fixture::new().await;
    let managed = f.create("codex", SessionKind::Managed).await;
    f.manager
        .set_config(
            &managed.id,
            "tether.agent",
            SessionConfigOptionValue::value_id("claude"),
        )
        .await
        .unwrap();
    for agent in ["codex", "claude"] {
        seed(&f, agent, "same-native-id", f.directory.path());
        std::fs::write(f.directory.path().join(agent).join("duplicate-list"), "").unwrap();
    }
    let page = f.manager.list(None).await.unwrap();
    assert!(page.failures.is_empty());
    assert_eq!(page.data.len(), 3);
    assert_eq!(page.data.iter().filter(|s| s.id == managed.id).count(), 1);
    assert_eq!(
        page.data
            .iter()
            .find(|s| s.id == managed.id)
            .unwrap()
            .bindings
            .len(),
        2
    );
    let mut ids: Vec<_> = page.data.into_iter().map(|s| s.id).collect();
    ids.sort();
    let next: Vec<_> = f
        .manager
        .list(None)
        .await
        .unwrap()
        .data
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(ids, next);
    f.close().await;
    let manager = Fixture::connect(f.directory.path()).await;
    let restored: Vec<_> = manager
        .list(None)
        .await
        .unwrap()
        .data
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(ids, restored);
    manager.shutdown().await;
}

#[tokio::test]
async fn discovered_ids_support_lifecycle_and_delete_real_native_records() {
    let f = Fixture::new().await;
    for agent in ["codex", "claude"] {
        seed(&f, agent, "external", f.directory.path());
    }
    let page = f.manager.list(None).await.unwrap();
    assert_eq!(page.data.len(), 2);
    for session in page.data {
        f.manager
            .load(&session.id, f.directory.path(), vec![])
            .await
            .unwrap();
        f.manager.resume(&session.id).await.unwrap();
        f.manager.close(&session.id).await.unwrap();
        f.manager
            .load(&session.id, f.directory.path(), vec![])
            .await
            .unwrap();
        let adopted = f
            .manager
            .adopt(AdoptSession {
                provider: session.provider.clone(),
                provider_session_id: "external".into(),
                kind: SessionKind::Managed,
            })
            .await
            .unwrap();
        assert_eq!(adopted.id, session.id);
        f.manager.delete(&session.id).await.unwrap();
        assert!(
            !f.directory
                .path()
                .join(&session.provider)
                .join("external.json")
                .exists()
        );
        assert!(
            !f.directory
                .path()
                .join(&session.provider)
                .join("external.history")
                .exists()
        );
    }
    assert!(f.manager.list(None).await.unwrap().data.is_empty());
    for agent in ["codex", "claude"] {
        assert!(
            !f.requests(agent)
                .iter()
                .any(|r| r["method"] == "session/delete")
        );
    }
    f.close().await;
}

#[tokio::test]
async fn catalog_drains_agent_pages_then_filters_before_public_pagination() {
    let f = Fixture::new().await;
    let codex = f.directory.path().join("project-a");
    let claude = f.directory.path().join("project-b");
    std::fs::create_dir(&codex).unwrap();
    std::fs::create_dir(&claude).unwrap();
    for (agent, cwd) in [("codex", &codex), ("claude", &claude)] {
        std::fs::write(f.directory.path().join(agent).join("page-size"), "7").unwrap();
        for index in 0..61 {
            seed(&f, agent, &format!("external-{index}"), cwd);
        }
    }
    let first = f.manager.list_filtered(None, Some(&claude)).await.unwrap();
    assert_eq!(first.data.len(), 50);
    assert!(first.data.iter().all(|s| s.provider == "claude"));
    let second = f
        .manager
        .list_filtered(first.next_cursor, Some(&claude))
        .await
        .unwrap();
    assert_eq!(second.data.len(), 11);
    assert!(second.next_cursor.is_none());
    assert!(
        first
            .data
            .iter()
            .all(|a| second.data.iter().all(|b| a.id != b.id))
    );
    for agent in ["codex", "claude"] {
        assert!(
            f.requests(agent)
                .iter()
                .filter(|r| r["method"] == "session/list")
                .count()
                >= 9
        );
    }
    f.close().await;
}

#[tokio::test]
async fn failed_catalog_refresh_preserves_cached_ids_and_reports_incompleteness() {
    let f = Fixture::new().await;
    seed(&f, "codex", "external", f.directory.path());
    let before = f.manager.list(None).await.unwrap().data[0].id.clone();
    std::fs::write(f.directory.path().join("codex/fail-list"), "").unwrap();
    let failed = f.manager.list(None).await.unwrap();
    assert_eq!(failed.data[0].id, before);
    assert_eq!(failed.failures[0].provider, "codex");
    std::fs::remove_file(f.directory.path().join("codex/fail-list")).unwrap();
    assert_eq!(f.manager.list(None).await.unwrap().data[0].id, before);
    f.close().await;
}

#[tokio::test]
async fn unsupported_binding_prevents_all_deletion_and_keeps_the_session_visible() {
    let directory = tempfile::tempdir().unwrap();
    let mut providers: Vec<Arc<dyn Provider>> = vec![];
    for agent in ["codex", "claude"] {
        let mut command = support::command(directory.path(), agent);
        if agent == "claude" {
            command.permanent_delete = None;
        }
        providers.push(Arc::new(
            AcpProvider::connect(command, &directory.path().join("state/agents"))
                .await
                .unwrap(),
        ));
    }
    let manager = Manager::open(
        directory.path().to_path_buf(),
        &directory.path().join("state/sessions.sqlite3"),
        providers,
    )
    .await
    .unwrap();
    let f = Fixture { directory, manager };
    let session = f.create("codex", SessionKind::Managed).await;
    f.manager
        .set_config(
            &session.id,
            "tether.agent",
            SessionConfigOptionValue::value_id("claude"),
        )
        .await
        .unwrap();
    assert!(matches!(
        f.manager.delete(&session.id).await,
        Err(Error::Deletion(_))
    ));
    let visible = f.manager.list(None).await.unwrap();
    assert_eq!(visible.data.len(), 1);
    assert_eq!(visible.data[0].id, session.id);
    assert!(
        visible.data[0]
            .deletion_error
            .as_ref()
            .unwrap()
            .contains("unsupported")
    );
    assert!(!f.directory.path().join("codex/delete-requests").exists());
    assert!(!f.directory.path().join("claude/delete-requests").exists());
    for binding in &visible.data[0].bindings {
        assert!(
            f.directory
                .path()
                .join(&binding.provider)
                .join(format!("{}.json", binding.session_id))
                .exists()
        );
    }
    f.close().await;
}

#[tokio::test]
async fn partial_deletion_remains_visible_across_restart_and_retries_only_remaining_bindings() {
    let f = Fixture::new().await;
    let session = f.create("codex", SessionKind::Managed).await;
    f.send(&session.id, None, "history to delete").await;
    f.finished(&session.id, 1).await;
    f.manager
        .set_config(
            &session.id,
            "tether.agent",
            SessionConfigOptionValue::value_id("claude"),
        )
        .await
        .unwrap();
    std::fs::write(f.directory.path().join("claude/fail-delete"), "").unwrap();
    assert!(matches!(
        f.manager.delete(&session.id).await,
        Err(Error::Deletion(_))
    ));
    let visible = f.manager.list(None).await.unwrap();
    assert_eq!(visible.data.len(), 1);
    assert_eq!(visible.data[0].id, session.id);
    assert!(visible.data[0].bindings[0].deleted);
    assert!(!visible.data[0].bindings[1].deleted);
    assert_eq!(visible.data[0].status, "deletion_failed");
    assert!(!f.manager.history(&session.id).await.unwrap().is_empty());
    assert!(matches!(
        f.manager
            .load(&session.id, f.directory.path(), vec![])
            .await,
        Err(Error::Deletion(_))
    ));
    f.close().await;
    let manager = Fixture::connect(f.directory.path()).await;
    assert_eq!(manager.list(None).await.unwrap().data[0].id, session.id);
    std::fs::remove_file(f.directory.path().join("claude/fail-delete")).unwrap();
    manager.delete(&session.id).await.unwrap();
    assert!(manager.list(None).await.unwrap().data.is_empty());
    assert_eq!(
        std::fs::read_to_string(f.directory.path().join("codex/delete-requests"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    manager.shutdown().await;
    let db = rusqlite::Connection::open(f.directory.path().join("state/sessions.sqlite3")).unwrap();
    for table in ["sessions", "inputs", "turns"] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn native_command_success_is_rejected_if_the_conversation_still_exists() {
    let f = Fixture::new().await;
    let session = f.create("codex", SessionKind::Managed).await;
    std::fs::write(f.directory.path().join("codex/noop-delete"), "").unwrap();
    assert!(matches!(
        f.manager.delete(&session.id).await,
        Err(Error::Deletion(_))
    ));
    let page = f.manager.list(None).await.unwrap();
    assert_eq!(page.data[0].id, session.id);
    assert!(
        page.data[0]
            .deletion_error
            .as_ref()
            .unwrap()
            .contains("still exists")
    );
    assert!(
        f.directory
            .path()
            .join("codex")
            .join(format!("{}.json", session.binding().session_id))
            .exists()
    );
    f.close().await;
}
