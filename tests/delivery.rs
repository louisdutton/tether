mod support;
use std::time::Duration;
use support::Fixture;
use tether::{model::QueuedMessage, model::SessionKind, store::Store};

#[tokio::test]
async fn steering_and_persistent_fifo_deliver_without_duplicate_authored_messages() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    f.send(&s.id, None, "slow original").await;
    let guided = f
        .manager
        .steer(&s.id, vec!["adjust the plan".into()])
        .await
        .unwrap();
    assert_eq!(guided["outcome"], "injected");
    let first = f
        .manager
        .enqueue(&s.id, "q1".into(), vec!["first followup".into()])
        .await
        .unwrap();
    let again = f
        .manager
        .enqueue(&s.id, "q1".into(), vec!["first followup".into()])
        .await
        .unwrap();
    assert_eq!(first, again);
    assert!(
        f.manager
            .enqueue(&s.id, "q1".into(), vec!["different".into()])
            .await
            .is_err()
    );
    f.manager
        .enqueue(&s.id, "q2".into(), vec!["second followup".into()])
        .await
        .unwrap();
    assert_eq!(f.manager.queued_messages(&s.id).unwrap().len(), 2);
    f.manager
        .steer(&s.id, vec!["finish original".into()])
        .await
        .unwrap();
    let history = f.finished(&s.id, 3).await;
    let texts: Vec<_> = history
        .iter()
        .flat_map(|t| t.content.items.iter())
        .filter(|i| i.kind == "message.user")
        .filter_map(|i| i.text.as_deref())
        .collect();
    for text in [
        "slow original",
        "adjust the plan",
        "finish original",
        "first followup",
        "second followup",
    ] {
        assert_eq!(
            texts.iter().filter(|t| **t == text).count(),
            1,
            "{text}: {texts:?}"
        );
    }
    let requests = f.requests("codex");
    let prompts: Vec<_> = requests
        .iter()
        .filter(|r| r["method"] == "session/prompt")
        .map(|r| r["params"]["prompt"][0]["text"].as_str().unwrap())
        .collect();
    assert_eq!(
        prompts,
        vec!["slow original", "first followup", "second followup"]
    );
    assert!(f.manager.queued_messages(&s.id).unwrap().is_empty());
    assert_eq!(
        f.manager
            .steer(&s.id, vec!["after finish".into()])
            .await
            .unwrap()["outcome"],
        "promptRequired"
    );
    f.close().await;
}

#[tokio::test]
async fn stop_pauses_backlog_until_explicitly_resumed() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    let turn = f.send(&s.id, None, "slow original").await;
    f.manager
        .enqueue(&s.id, "q".into(), vec!["followup".into()])
        .await
        .unwrap();
    f.manager.interrupt(&s.id, &turn).await.unwrap();
    f.finished(&s.id, 1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(f.manager.queued_messages(&s.id).unwrap()[0].state, "paused");
    assert_eq!(
        f.requests("codex")
            .iter()
            .filter(|r| r["method"] == "session/prompt")
            .count(),
        1
    );
    f.manager
        .change_queued_message(&s.id, "q", true)
        .await
        .unwrap();
    f.finished(&s.id, 2).await;
    f.close().await;
}

#[tokio::test]
async fn backlog_survives_restart_and_cancelled_receipts_cannot_reenqueue() {
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    f.send(&s.id, None, "slow original").await;
    f.manager
        .enqueue(&s.id, "q".into(), vec!["kept across restart".into()])
        .await
        .unwrap();
    f.close().await;
    let manager = Fixture::connect(f.directory.path()).await;
    let queued = manager.queued_messages(&s.id).unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].state, "paused");
    manager
        .change_queued_message(&s.id, "q", false)
        .await
        .unwrap();
    let receipt = manager
        .enqueue(&s.id, "q".into(), vec!["kept across restart".into()])
        .await
        .unwrap();
    assert_eq!(receipt.state, "cancelled");
    assert!(manager.queued_messages(&s.id).unwrap().is_empty());
    manager.shutdown().await;
}

#[test]
fn interrupted_dispatch_is_retained_as_uncertain_and_session_deletion_cleans_backlog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = Store::open(&path).unwrap();
        db.queue_message(&QueuedMessage {
            message_id: "m".into(),
            session_id: "s".into(),
            prompt: vec!["do something".into()],
            state: "queued".into(),
            error: None,
        })
        .unwrap();
        db.queue_state("m", "dispatching", None).unwrap();
    }
    let db = Store::open(&path).unwrap();
    assert_eq!(db.queued(Some("s")).unwrap()[0].state, "uncertain");
    db.delete("s").unwrap();
    assert!(db.queued(Some("s")).unwrap().is_empty());
}
