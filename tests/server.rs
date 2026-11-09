//! Provider/manager scenarios, independent of the client wire protocol.
mod support;
use std::time::Duration;
use support::Fixture;
use tether::{
    manager::Error,
    model::{AdoptSession, CreateSession, Message, SessionKind},
    providers::acp::{AcpProvider, AgentCommand},
};

#[tokio::test]
async fn workspace_and_provider_boundaries() {
    let f = Fixture::new().await;
    for (provider, cwd) in [
        ("missing", f.directory.path().to_str().unwrap()),
        ("codex", "/"),
    ] {
        assert!(matches!(
            f.manager
                .create(CreateSession {
                    provider: provider.into(),
                    cwd: cwd.into(),
                    kind: SessionKind::Managed,
                    mcp_servers: vec![]
                })
                .await,
            Err(Error::Invalid(_))
        ));
    }
    std::os::unix::fs::symlink("/", f.directory.path().join("escape")).unwrap();
    assert!(
        f.manager
            .create(CreateSession {
                provider: "codex".into(),
                cwd: f.directory.path().join("escape").to_string_lossy().into(),
                kind: SessionKind::Managed,
                mcp_servers: vec![]
            })
            .await
            .is_err()
    );
    let s = f.create("codex", SessionKind::Managed).await;
    assert_ne!(s.id, s.bindings[0].session_id);
    assert!(matches!(
        f.manager.get("fixture-session").await,
        Err(Error::NotFound)
    ));
    assert!(
        f.manager
            .message(
                &s.id,
                Message {
                    text: " ".into(),
                    provider: None
                }
            )
            .await
            .is_err()
    );
    f.close().await;
}

#[tokio::test]
async fn captured_history_is_readable_after_provider_failure() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    let mut events = f.manager.subscribe();
    f.send(&s.id, None, "hello; $(touch injected)").await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while events.recv().await.unwrap().kind != "turn.completed" {}
    })
    .await
    .unwrap();
    f.send(&s.id, None, "crash").await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while f
            .manager
            .providers()
            .iter()
            .find(|p| p.id == "codex")
            .unwrap()
            .ready
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let history = f.manager.history(&s.id).await.unwrap();
    assert_eq!(
        history[0].content.items[0].text.as_deref(),
        Some("hello; $(touch injected)")
    );
    assert!(
        history[0]
            .content
            .items
            .iter()
            .any(|i| i.text.as_deref() == Some("hello"))
    );
    assert!(!f.directory.path().join("injected").exists());
    assert_eq!(f.manager.get(&s.id).await.unwrap().status, "unavailable");
    assert!(f.manager.ready()); // Claude remains available.
    f.close().await;
}

#[tokio::test]
async fn active_turn_blocks_switch_and_interrupt_maps_native_id() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    let turn = f.send(&s.id, None, "slow").await;
    assert!(matches!(
        f.manager
            .message(
                &s.id,
                Message {
                    provider: Some("claude".into()),
                    text: "review".into()
                }
            )
            .await,
        Err(Error::Busy)
    ));
    assert!(matches!(f.manager.resume(&s.id).await, Err(Error::Busy)));
    assert!(
        !f.requests("claude")
            .iter()
            .any(|r| r["method"] == "session/new")
    );
    f.manager.interrupt(&s.id, &turn).await.unwrap();
    assert_eq!(f.finished(&s.id, 1).await[0].content.status, "interrupted");
    f.close().await;
}

#[tokio::test]
async fn switches_keep_identity_and_replay_only_away_context() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    f.send(&s.id, None, "original Codex request").await;
    f.finished(&s.id, 1).await;
    f.send(&s.id, Some("claude"), "review with Claude").await;
    let history = f.finished(&s.id, 2).await;
    assert_eq!(history[1].provider, "claude");
    assert_eq!(
        history[1].content.items[0].text.as_deref(),
        Some("review with Claude")
    );
    assert_eq!(
        history[1]
            .content
            .items
            .iter()
            .filter(|i| i.provider_data["sessionUpdate"] == "agent_message_chunk")
            .count(),
        1
    );
    let native = f.manager.get(&s.id).await.unwrap().bindings[1]
        .session_id
        .clone();
    f.send(&s.id, Some("codex"), "back to Codex").await;
    f.finished(&s.id, 3).await;
    let requests = f.requests("codex");
    let last = requests
        .iter()
        .rfind(|r| r["method"] == "session/prompt")
        .unwrap();
    let prompt = last["params"]["prompt"][0]["text"].as_str().unwrap();
    assert!(prompt.contains("hello"));
    assert!(!prompt.contains("original Codex request"));
    f.send(&s.id, Some("claude"), "return").await;
    f.finished(&s.id, 4).await;
    let session = f.manager.get(&s.id).await.unwrap();
    assert_eq!(session.id, s.id);
    assert_eq!(session.bindings[1].session_id, native);
    assert_eq!(session.agent_changes.len(), 3);
    assert_eq!(
        f.requests("claude")
            .iter()
            .filter(|r| r["method"] == "session/new")
            .count(),
        1
    );
    f.close().await;
}

#[tokio::test]
async fn direct_import_and_adoption_preserve_ownership() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Direct).await;
    assert!(
        f.manager
            .adopt(AdoptSession {
                provider: "codex".into(),
                provider_session_id: s.bindings[0].session_id.clone(),
                kind: SessionKind::Managed
            })
            .await
            .is_err()
    );
    assert!(
        f.manager
            .message(
                &s.id,
                Message {
                    provider: Some("claude".into()),
                    text: "switch".into()
                }
            )
            .await
            .is_err()
    );
    let imported = f.manager.import(&s.id).await.unwrap();
    assert_eq!(imported.id, s.id);
    assert_eq!(imported.kind, SessionKind::Managed);
    f.send(&s.id, Some("claude"), "switch after import").await;
    f.finished(&s.id, 1).await;
    f.close().await;
}

#[tokio::test]
async fn switched_session_survives_restart() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    f.send(&s.id, None, "first").await;
    f.finished(&s.id, 1).await;
    f.send(&s.id, Some("claude"), "review").await;
    f.finished(&s.id, 2).await;
    f.close().await;
    let restarted = Fixture::connect(f.directory.path()).await;
    assert_eq!(restarted.get(&s.id).await.unwrap().provider, "claude");
    assert_eq!(restarted.history(&s.id).await.unwrap().len(), 2);
    restarted
        .message(
            &s.id,
            Message {
                provider: None,
                text: "after restart".into(),
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let history = restarted.history(&s.id).await.unwrap();
            if history.len() == 3 && history[2].content.status == "completed" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    restarted.shutdown().await;
}

#[tokio::test]
async fn unacknowledged_delivery_retains_handover() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    f.send(&s.id, None, "context that must survive").await;
    f.finished(&s.id, 1).await;
    f.send(&s.id, Some("claude"), "failure").await;
    assert_eq!(f.finished(&s.id, 2).await[1].content.status, "failed");
    assert!(
        f.manager
            .get(&s.id)
            .await
            .unwrap()
            .pending_context
            .is_some()
    );
    f.send(&s.id, None, "new request").await;
    f.finished(&s.id, 3).await;
    assert_eq!(
        f.requests("claude")
            .iter()
            .filter(|r| r["method"] == "session/prompt"
                && r["params"]["prompt"][0]["text"]
                    .as_str()
                    .is_some_and(|s| s.contains("context that must survive")))
            .count(),
        2
    );
    f.close().await;
}

#[tokio::test]
async fn missing_provider_executable_fails_startup() {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        AcpProvider::connect(
            AgentCommand {
                id: "missing".into(),
                command: "/does/not/exist".into(),
                args: vec![]
            },
            dir.path()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn settings_saved_during_a_turn_do_not_leave_a_running_session_after_restart() {
    use agent_client_protocol::v1::schema::SessionConfigOptionValue;
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    f.send(&s.id, None, "slow").await;
    f.manager
        .set_config(
            &s.id,
            "agent/model",
            SessionConfigOptionValue::value_id("large"),
        )
        .await
        .unwrap();
    f.close().await;
    let manager = Fixture::connect(f.directory.path()).await;
    assert_ne!(manager.get(&s.id).await.unwrap().status, "running");
    let history = manager.history(&s.id).await.unwrap();
    assert_eq!(history[0].content.status, "failed");
    manager
        .load(&s.id, f.directory.path(), vec![])
        .await
        .unwrap();
    let options = manager.config(&s.id).await.unwrap();
    assert_eq!(
        serde_json::to_value(
            options
                .iter()
                .find(|o| o.id.to_string() == "agent/model")
                .unwrap()
        )
        .unwrap()["currentValue"],
        "large"
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn native_acp_discovery_and_load_can_import_an_existing_conversation() {
    use serde_json::json;
    let f = Fixture::new().await;
    let native = "codex-external";
    let directory = f.directory.path().join("codex");
    std::fs::write(
        directory.join(format!("{native}.json")),
        serde_json::to_vec(&json!({"sessionId":native,"cwd":f.directory.path()})).unwrap(),
    )
    .unwrap();
    let history = [
        json!({"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"external original request"}}),
        json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"external answer"}}),
    ].into_iter().map(|u| format!("{u}\n")).collect::<String>();
    std::fs::write(directory.join(format!("{native}.history")), history).unwrap();
    let discovered = f.manager.discover("codex", None).await.unwrap();
    assert!(discovered.data.iter().any(|s| s.id == native));
    let s = f
        .manager
        .adopt(AdoptSession {
            provider: "codex".into(),
            provider_session_id: native.into(),
            kind: SessionKind::Managed,
        })
        .await
        .unwrap();
    assert_eq!(f.manager.history(&s.id).await.unwrap().len(), 1);
    f.manager
        .set_config(
            &s.id,
            "tether.agent",
            agent_client_protocol::v1::schema::SessionConfigOptionValue::value_id("claude"),
        )
        .await
        .unwrap();
    f.send(&s.id, None, "continue").await;
    f.finished(&s.id, 2).await;
    let requests = f.requests("claude");
    let prompt = requests
        .iter()
        .rfind(|r| r["method"] == "session/prompt")
        .unwrap()["params"]["prompt"][0]["text"]
        .as_str()
        .unwrap();
    assert!(prompt.contains("external original request"));
    assert!(prompt.contains("external answer"));
    f.close().await;
}
