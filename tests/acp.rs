mod support;
use agent_client_protocol::v1::{ConnectionTo, schema::*};
use agent_client_protocol::{
    Agent, ByteStreams, Channel, Client, ConnectTo, UntypedMessage, schema::ProtocolVersion,
};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use support::Fixture;
use tokio_util::{
    compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt},
    sync::CancellationToken,
};

struct Peer {
    cx: ConnectionTo<Agent>,
    updates: Arc<Mutex<Vec<SessionNotification>>>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<agent_client_protocol::Result<()>>,
}
impl Peer {
    async fn connect(transport: impl ConnectTo<Client> + 'static) -> Self {
        let stop = CancellationToken::new();
        let done = stop.clone();
        let updates = Arc::new(Mutex::new(Vec::new()));
        let received = updates.clone();
        let (ready, connection) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            Client
                .v1()
                .on_receive_notification(
                    async move |update: SessionNotification, _cx| {
                        received.lock().unwrap().push(update);
                        Ok(())
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .connect_with(transport, async move |cx| {
                    ready.send(cx).unwrap();
                    done.cancelled().await;
                    Ok(())
                })
                .await
        });
        Self {
            cx: connection.await.unwrap(),
            updates,
            stop,
            task,
        }
    }
    async fn initialize(&self) -> InitializeResponse {
        self.cx
            .send_request(InitializeRequest::new(ProtocolVersion::V1))
            .block_task()
            .await
            .unwrap()
    }
    async fn new_session(&self, cwd: &std::path::Path) -> SessionId {
        self.cx
            .send_request(NewSessionRequest::new(cwd))
            .block_task()
            .await
            .unwrap()
            .session_id
    }
    async fn close(self) {
        self.stop.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap();
    }
}
async fn connected(
    f: &Fixture,
) -> (
    Peer,
    CancellationToken,
    tokio::task::JoinHandle<agent_client_protocol::Result<()>>,
) {
    let (client, server) = Channel::duplex();
    let stop = CancellationToken::new();
    let manager = f.manager.clone();
    let done = stop.clone();
    let task = tokio::spawn(async move { tether::acp::serve(server, manager, done).await });
    (Peer::connect(client).await, stop, task)
}
async fn close(
    f: Fixture,
    peer: Peer,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<agent_client_protocol::Result<()>>,
) {
    peer.close().await;
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap();
    f.close().await;
}

#[tokio::test]
async fn official_client_initializes_creates_prompts_streams_and_loads() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    let init = peer.initialize().await;
    assert_eq!(init.protocol_version, ProtocolVersion::V1);
    assert!(init.agent_capabilities.load_session);
    assert!(init.agent_capabilities.session_capabilities.list.is_some());
    assert!(init.auth_methods.is_empty());
    let id = peer.new_session(f.directory.path()).await;
    assert!(
        peer.updates
            .lock()
            .unwrap()
            .iter()
            .any(|u| matches!(u.update, SessionUpdate::AvailableCommandsUpdate(_)))
    );
    let result = peer
        .cx
        .send_request(PromptRequest::new(id.clone(), vec!["hello".into()]))
        .block_task()
        .await
        .unwrap();
    assert_eq!(result.stop_reason, StopReason::EndTurn);
    // Notifications must precede the prompt response, without appending completed text twice.
    let text: String = peer
        .updates
        .lock()
        .unwrap()
        .iter()
        .filter_map(|u| match &u.update {
            SessionUpdate::AgentMessageChunk(c) => match &c.content {
                ContentBlock::Text(t) => Some(t.text.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(text, "hello");
    let list = peer
        .cx
        .send_request(ListSessionsRequest::new())
        .block_task()
        .await
        .unwrap();
    assert_eq!(list.sessions[0].session_id, id);
    peer.updates.lock().unwrap().clear();
    peer.cx
        .send_request(LoadSessionRequest::new(id.clone(), f.directory.path()))
        .block_task()
        .await
        .unwrap();
    let updates = peer.updates.lock().unwrap().clone();
    assert!(
        updates
            .iter()
            .any(|u| matches!(u.update, SessionUpdate::UserMessageChunk(_)))
    );
    assert!(
        updates
            .iter()
            .any(|u| matches!(u.update, SessionUpdate::AgentMessageChunk(_)))
    );
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn prompt_does_not_block_cancellation_or_other_requests() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    peer.initialize().await;
    let id = peer.new_session(f.directory.path()).await;
    let pending = peer
        .cx
        .send_request(PromptRequest::new(id.clone(), vec!["slow".into()]))
        .block_task();
    let pending = tokio::spawn(pending);
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.manager.get(&id.to_string()).await.unwrap().status != "running" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let overlap = peer
        .cx
        .send_request(PromptRequest::new(id.clone(), vec!["overlap".into()]))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(overlap.code.value(), -32001);
    peer.cx
        .send_notification(CancelNotification::new(id.clone()))
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.stop_reason, StopReason::Cancelled);
    assert_eq!(
        peer.cx
            .send_request(ListSessionsRequest::new())
            .block_task()
            .await
            .unwrap()
            .sessions
            .len(),
        1
    );
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn initialization_attachment_content_and_method_errors_are_explicit() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    assert!(
        peer.cx
            .send_request(NewSessionRequest::new(f.directory.path()))
            .block_task()
            .await
            .is_err()
    );
    peer.initialize().await;
    let unknown = peer
        .cx
        .send_request(UntypedMessage::new("made/up", json!({})).unwrap())
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(unknown.code.value(), -32601);
    let unattached = peer
        .cx
        .send_request(PromptRequest::new("missing", vec!["hello".into()]))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(unattached.code.value(), -32002);
    assert!(
        peer.cx
            .send_request(NewSessionRequest::new("/"))
            .block_task()
            .await
            .is_err()
    );
    let id = peer.new_session(f.directory.path()).await;
    let wrong_cwd = peer
        .cx
        .send_request(LoadSessionRequest::new(id.clone(), "/"))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(wrong_cwd.code.value(), -32602);
    let image: ContentBlock =
        serde_json::from_value(json!({"type":"image","data":"eA==","mimeType":"image/png"}))
            .unwrap();
    peer.cx
        .send_request(PromptRequest::new(id.clone(), vec![image]))
        .block_task()
        .await
        .unwrap();
    // An invalid prompt releases the per-session reservation and doesn't wedge the connection.
    let reference: ContentBlock = serde_json::from_value(
        json!({"type":"resource_link","uri":"file:///project/README.md","name":"README.md"}),
    )
    .unwrap();
    assert_eq!(
        peer.cx
            .send_request(PromptRequest::new(id, vec!["valid".into(), reference]))
            .block_task()
            .await
            .unwrap()
            .stop_reason,
        StopReason::EndTurn
    );
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn provider_switches_and_tools_use_acp_updates_and_namespaced_metadata() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    peer.initialize().await;
    let id = peer.new_session(f.directory.path()).await;
    peer.cx
        .send_request(PromptRequest::new(id.clone(), vec!["first".into()]))
        .block_task()
        .await
        .unwrap();
    peer.updates.lock().unwrap().clear();
    peer.cx
        .send_request(SetSessionConfigOptionRequest::new(
            id.clone(),
            "tether.agent",
            SessionConfigOptionValue::value_id("claude"),
        ))
        .block_task()
        .await
        .unwrap();
    let response = peer
        .cx
        .send_request(PromptRequest::new(id.clone(), vec!["review".into()]))
        .block_task()
        .await
        .unwrap();
    assert_eq!(response.stop_reason, StopReason::EndTurn);
    let text: String = peer
        .updates
        .lock()
        .unwrap()
        .iter()
        .filter_map(|u| match &u.update {
            SessionUpdate::AgentMessageChunk(c) => match &c.content {
                ContentBlock::Text(t) => Some(t.text.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(text, "hello");
    assert!(
        peer.updates
            .lock()
            .unwrap()
            .iter()
            .any(|u| matches!(u.update, SessionUpdate::ToolCall(_)))
    );
    let history = f.manager.history(&id.to_string()).await.unwrap();
    assert_eq!(history[1].provider, "claude");
    assert_eq!(history[1].content.items[0].text.as_deref(), Some("review"));
    assert_eq!(f.manager.agent_changes(&id.to_string()).unwrap().len(), 1);
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn baseline_stdio_mcp_configuration_reaches_both_native_adapters() {
    for provider in ["codex", "claude"] {
        let f = Fixture::new().await;
        let (peer, stop, task) = connected(&f).await;
        peer.initialize().await;
        let mcp: McpServer = serde_json::from_value(json!({"name":"fixture","command":"/bin/echo","args":["mcp"],"env":[{"name":"TEST_SETTING","value":"value"}]})).unwrap();
        let session = peer
            .cx
            .send_request(NewSessionRequest::new(f.directory.path()).mcp_servers(vec![mcp]))
            .block_task()
            .await
            .unwrap();
        if provider == "claude" {
            peer.cx
                .send_request(SetSessionConfigOptionRequest::new(
                    session.session_id.clone(),
                    "tether.agent",
                    SessionConfigOptionValue::value_id(provider),
                ))
                .block_task()
                .await
                .unwrap();
        }
        peer.cx
            .send_request(PromptRequest::new(
                session.session_id.clone(),
                vec!["hello".into()],
            ))
            .block_task()
            .await
            .unwrap();
        let recorded = serde_json::to_string(&f.requests(provider)).unwrap();
        assert!(recorded.contains("TEST_SETTING"));
        assert!(recorded.contains("/bin/echo"));
        let stored = f
            .manager
            .get(&session.session_id.to_string())
            .await
            .unwrap();
        assert_eq!(stored.mcp_servers.len(), 1);
        close(f, peer, stop, task).await;
    }
}

#[tokio::test]
async fn real_websocket_transport_works_with_the_official_sdk() {
    use futures::{SinkExt, StreamExt};
    let f = Fixture::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let app = tether::api::router(tether::api::AppState {
        manager: f.manager.clone(),
        shutdown: shutdown.clone(),
    });
    let done = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(done.cancelled_owned())
            .await
    });
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/acp"))
        .await
        .unwrap();
    let (writer, reader) = socket.split();
    let outgoing = writer
        .sink_map_err(std::io::Error::other)
        .with(|text: String| {
            futures::future::ready(Ok::<_, std::io::Error>(
                tokio_tungstenite::tungstenite::Message::Text(text.into()),
            ))
        });
    let incoming = reader.filter_map(|message| {
        futures::future::ready(match message {
            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => Some(Ok(text.to_string())),
            Err(error) => Some(Err(std::io::Error::other(error))),
            _ => None,
        })
    });
    let peer = Peer::connect(agent_client_protocol::Lines::new(outgoing, incoming)).await;
    peer.initialize().await;
    let id = peer.new_session(f.directory.path()).await;
    assert_eq!(
        peer.cx
            .send_request(PromptRequest::new(id, vec!["hello".into()]))
            .block_task()
            .await
            .unwrap()
            .stop_reason,
        StopReason::EndTurn
    );
    peer.close().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.close().await;
}

#[tokio::test]
async fn executable_stdio_is_clean_acp_and_needs_no_app_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_tether"))
        .arg("--stdio")
        .env("TETHER_WORKSPACE_ROOT", directory.path())
        .env("TETHER_DATA_DIR", directory.path().join("state"))
        .env("TETHER_AGENTS", serde_json::to_string(&json!([{"id":"codex","command":support::command(directory.path(), "codex").command,"args":support::command(directory.path(), "codex").args}])).unwrap())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let peer = Peer::connect(ByteStreams::new(
        child.stdin.take().unwrap().compat_write(),
        child.stdout.take().unwrap().compat(),
    ))
    .await;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        peer.initialize().await;
        let id = peer.new_session(directory.path()).await;
        peer.cx
            .send_request(PromptRequest::new(id, vec!["hello".into()]))
            .block_task()
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(result.stop_reason, StopReason::EndTurn);
    peer.close().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

#[tokio::test]
async fn reconnect_attaches_running_work_and_another_client_can_cancel_it() {
    let f = Fixture::new().await;
    let (first, first_stop, first_task) = connected(&f).await;
    first.initialize().await;
    let id = first.new_session(f.directory.path()).await;
    let pending = tokio::spawn(
        first
            .cx
            .send_request(PromptRequest::new(id.clone(), vec!["slow".into()]))
            .block_task(),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.manager.get(&id.to_string()).await.unwrap().status != "running" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.close().await;
    first_stop.cancel();
    let _ = first_task.await;
    assert_eq!(
        f.manager.get(&id.to_string()).await.unwrap().status,
        "running"
    );
    let (second, stop, task) = connected(&f).await;
    second.initialize().await;
    second
        .cx
        .send_request(LoadSessionRequest::new(id.clone(), f.directory.path()))
        .block_task()
        .await
        .unwrap();
    second
        .cx
        .send_notification(CancelNotification::new(id.clone()))
        .unwrap();
    f.finished(&id.to_string(), 1).await;
    let _ = pending.await;
    let requests = f.requests("codex");
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "session/prompt")
            .count(),
        1
    );
    assert!(requests.iter().any(|r| r["method"] == "session/cancel"));
    close(f, second, stop, task).await;
}

#[tokio::test]
async fn standard_settings_preserve_catalogs_validate_values_and_restore_after_restart() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    peer.initialize().await;
    let response = peer
        .cx
        .send_request(NewSessionRequest::new(f.directory.path()))
        .block_task()
        .await
        .unwrap();
    let id = response.session_id;
    let options = response.config_options.unwrap();
    assert_eq!(options[0].id.to_string(), "tether.agent");
    assert!(
        options
            .iter()
            .any(|o| o.category == Some(SessionConfigOptionCategory::Model)
                && o.id.to_string() == "agent/model")
    );
    for (key, value) in [("agent/model", "large"), ("agent/effort", "high")] {
        let response = peer
            .cx
            .send_request(SetSessionConfigOptionRequest::new(
                id.clone(),
                key,
                SessionConfigOptionValue::value_id(value),
            ))
            .block_task()
            .await
            .unwrap();
        let option = response
            .config_options
            .iter()
            .find(|o| o.id.to_string() == key)
            .unwrap();
        assert_eq!(serde_json::to_value(option).unwrap()["currentValue"], value);
    }
    assert!(
        peer.cx
            .send_request(SetSessionConfigOptionRequest::new(
                id.clone(),
                "agent/model",
                SessionConfigOptionValue::value_id("unknown")
            ))
            .block_task()
            .await
            .is_err()
    );
    assert!(
        peer.cx
            .send_request(SetSessionConfigOptionRequest::new(
                id.clone(),
                "tether.agent",
                SessionConfigOptionValue::value_id("unknown")
            ))
            .block_task()
            .await
            .is_err()
    );
    assert!(
        peer.cx
            .send_request(
                UntypedMessage::new("_tether/session/history", json!({"sessionId":id})).unwrap()
            )
            .block_task()
            .await
            .is_err()
    );
    peer.close().await;
    stop.cancel();
    let _ = task.await;
    f.close().await;
    let manager = Fixture::connect(f.directory.path()).await;
    manager
        .load(&id.to_string(), f.directory.path(), vec![])
        .await
        .unwrap();
    let options = manager.config(&id.to_string()).await.unwrap();
    for (key, value) in [("agent/model", "large"), ("agent/effort", "high")] {
        let option = options.iter().find(|o| o.id.to_string() == key).unwrap();
        assert_eq!(serde_json::to_value(option).unwrap()["currentValue"], value);
    }
    let requests = f.requests("codex");
    assert!(requests.iter().any(|r| r["method"] == "session/load"));
    manager.shutdown().await;
}

#[tokio::test]
async fn acp_content_and_structured_updates_survive_transport_and_history() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    let init = peer.initialize().await;
    assert!(init.agent_capabilities.prompt_capabilities.image);
    let id = peer.new_session(f.directory.path()).await;
    let image: ContentBlock =
        serde_json::from_value(json!({"type":"image","data":"eA==","mimeType":"image/png"}))
            .unwrap();
    let link: ContentBlock = serde_json::from_value(
        json!({"type":"resource_link","uri":"file:///README.md","name":"README.md"}),
    )
    .unwrap();
    let content = vec!["limit".into(), image.clone(), link.clone()];
    let response = peer
        .cx
        .send_request(PromptRequest::new(id.clone(), content.clone()))
        .block_task()
        .await
        .unwrap();
    assert_eq!(response.stop_reason, StopReason::MaxTokens);
    let requests = f.requests("codex");
    let native = requests
        .iter()
        .rfind(|r| r["method"] == "session/prompt")
        .unwrap();
    assert_eq!(
        native["params"]["prompt"],
        serde_json::to_value(content).unwrap()
    );
    let live = peer.updates.lock().unwrap().clone();
    let expected_path = f.directory.path().join("README.md");
    assert!(
        live.iter()
            .any(|u| matches!(&u.update, SessionUpdate::ToolCall(c)
        if c.locations.first().is_some_and(|location| location.path == expected_path)))
    );
    assert!(
        live.iter()
            .any(|u| matches!(u.update, SessionUpdate::AgentThoughtChunk(_)))
    );
    assert!(
        live.iter()
            .any(|u| matches!(u.update, SessionUpdate::Plan(_)))
    );
    assert!(live.iter().any(|u| matches!(&u.update, SessionUpdate::ToolCall(c) if c.raw_input == Some(json!({"path":"README.md"})))));
    assert!(live.iter().any(|u| matches!(&u.update, SessionUpdate::ToolCallUpdate(c) if serde_json::to_value(c).unwrap()["content"][0]["type"] == "diff")));
    peer.updates.lock().unwrap().clear();
    peer.cx
        .send_request(LoadSessionRequest::new(id.clone(), f.directory.path()))
        .block_task()
        .await
        .unwrap();
    let replay = peer.updates.lock().unwrap().clone();
    assert!(
        replay
            .iter()
            .any(|u| matches!(&u.update, SessionUpdate::ToolCallUpdate(c)
        if c.fields.content.as_ref().is_some_and(|content| content.iter().any(|content|
            matches!(content, ToolCallContent::Diff(diff) if diff.path == expected_path)))))
    );
    assert!(
        replay
            .iter()
            .any(|u| matches!(&u.update, SessionUpdate::UserMessageChunk(c) if c.content == image))
    );
    assert!(
        replay
            .iter()
            .any(|u| matches!(&u.update, SessionUpdate::UserMessageChunk(c) if c.content == link))
    );
    assert!(
        replay
            .iter()
            .any(|u| matches!(u.update, SessionUpdate::Plan(_)))
    );
    let text = |updates: &[SessionNotification]| {
        updates
            .iter()
            .filter_map(|u| match &u.update {
                SessionUpdate::AgentMessageChunk(c) => match &c.content {
                    ContentBlock::Text(t) => Some(t.text.as_str()),
                    _ => None,
                },
                _ => None,
            })
            .collect::<String>()
    };
    assert_eq!(text(&replay), "hello");
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn cached_history_loads_when_its_agent_is_removed_from_configuration() {
    use tether::{manager::Manager, model::SessionKind, providers::acp::AcpProvider};
    let f = Fixture::new().await;
    let s = f.create("codex", SessionKind::Managed).await;
    f.send(&s.id, None, "saved input").await;
    f.finished(&s.id, 1).await;
    f.close().await;
    let remaining = AcpProvider::connect(
        support::command(f.directory.path(), "claude"),
        &f.directory.path().join("state/agents"),
    )
    .await
    .unwrap();
    let manager = Manager::open(
        f.directory.path().canonicalize().unwrap(),
        &f.directory.path().join("state/sessions.sqlite3"),
        vec![Arc::new(remaining)],
    )
    .await
    .unwrap();
    let (client, server) = Channel::duplex();
    let stop = CancellationToken::new();
    let done = stop.clone();
    let running = manager.clone();
    let task = tokio::spawn(async move { tether::acp::serve(server, running, done).await });
    let peer = Peer::connect(client).await;
    peer.initialize().await;
    let response = peer
        .cx
        .send_request(LoadSessionRequest::new(s.id, f.directory.path()))
        .block_task()
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&response.config_options.unwrap()[0]).unwrap()["currentValue"],
        "codex"
    );
    assert!(peer.updates.lock().unwrap().iter().any(|u| matches!(&u.update, SessionUpdate::AgentMessageChunk(c) if matches!(&c.content, ContentBlock::Text(t) if t.text == "hello"))));
    peer.close().await;
    stop.cancel();
    let _ = task.await;
    manager.shutdown().await;
}

#[tokio::test]
async fn deletion_is_advertised_idempotent_and_persistent() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    let init = peer.initialize().await;
    assert!(
        init.agent_capabilities
            .session_capabilities
            .delete
            .is_some()
    );
    let id = peer.new_session(f.directory.path()).await;
    peer.cx
        .send_request(PromptRequest::new(id.clone(), vec!["hello".into()]))
        .block_task()
        .await
        .unwrap();
    for target in [id.clone(), id.clone(), SessionId::new("never-existed")] {
        peer.cx
            .send_request(DeleteSessionRequest::new(target))
            .block_task()
            .await
            .unwrap();
    }
    assert!(
        peer.cx
            .send_request(ListSessionsRequest::new())
            .block_task()
            .await
            .unwrap()
            .sessions
            .is_empty()
    );
    assert!(matches!(
        f.manager.history(&id.to_string()).await,
        Err(tether::manager::Error::NotFound)
    ));
    let missing = peer
        .cx
        .send_request(LoadSessionRequest::new(id.clone(), f.directory.path()))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(missing.code.value(), -32002);
    peer.close().await;
    stop.cancel();
    let _ = task.await;
    f.close().await;
    let manager = Fixture::connect(f.directory.path()).await;
    assert!(manager.list(None).await.unwrap().data.is_empty());
    manager.shutdown().await;
    assert!(
        !f.requests("codex")
            .iter()
            .any(|r| r["method"] == "session/delete")
    );
}

#[tokio::test]
async fn resume_attaches_without_replaying_history_and_validates_cwd() {
    let f = Fixture::new().await;
    let session = f.create("codex", tether::model::SessionKind::Managed).await;
    f.send(&session.id, None, "hello").await;
    f.finished(&session.id, 1).await;
    let (peer, stop, task) = connected(&f).await;
    assert!(
        peer.initialize()
            .await
            .agent_capabilities
            .session_capabilities
            .resume
            .is_some()
    );
    let error = peer
        .cx
        .send_request(ResumeSessionRequest::new(session.id.clone(), "/"))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(error.code.value(), -32602);
    peer.cx
        .send_request(ResumeSessionRequest::new(
            session.id.clone(),
            f.directory.path(),
        ))
        .block_task()
        .await
        .unwrap();
    assert!(!peer.updates.lock().unwrap().iter().any(|u| matches!(
        u.update,
        SessionUpdate::UserMessageChunk(_) | SessionUpdate::AgentMessageChunk(_)
    )));
    peer.cx
        .send_request(PromptRequest::new(session.id, vec!["continue".into()]))
        .block_task()
        .await
        .unwrap();
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn modes_are_returned_changed_and_reloaded_using_standard_methods() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    peer.initialize().await;
    let created = peer
        .cx
        .send_request(NewSessionRequest::new(f.directory.path()))
        .block_task()
        .await
        .unwrap();
    assert_eq!(created.modes.unwrap().current_mode_id.to_string(), "ask");
    let id = created.session_id;
    peer.cx
        .send_request(SetSessionModeRequest::new(id.clone(), "code"))
        .block_task()
        .await
        .unwrap();
    let error = peer
        .cx
        .send_request(SetSessionModeRequest::new(id.clone(), "nonexistent"))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(error.code.value(), -32602);
    let loaded = peer
        .cx
        .send_request(LoadSessionRequest::new(id, f.directory.path()))
        .block_task()
        .await
        .unwrap();
    assert_eq!(loaded.modes.unwrap().current_mode_id.to_string(), "code");
    assert!(
        f.requests("codex")
            .iter()
            .any(|r| r["method"] == "session/set_mode" && r["params"]["modeId"] == "code")
    );
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn close_cancels_work_retains_history_and_requires_reattachment() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    assert!(
        peer.initialize()
            .await
            .agent_capabilities
            .session_capabilities
            .close
            .is_some()
    );
    let id = peer.new_session(f.directory.path()).await;
    let pending = tokio::spawn(
        peer.cx
            .send_request(PromptRequest::new(id.clone(), vec!["slow".into()]))
            .block_task(),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.manager.get(&id.to_string()).await.unwrap().status != "running" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let busy = peer
        .cx
        .send_request(DeleteSessionRequest::new(id.clone()))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(busy.code.value(), -32001);
    tokio::time::timeout(
        Duration::from_secs(3),
        peer.cx
            .send_request(CloseSessionRequest::new(id.clone()))
            .block_task(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        pending.await.unwrap().unwrap().stop_reason,
        StopReason::Cancelled
    );
    assert_eq!(
        f.manager.get(&id.to_string()).await.unwrap().status,
        "unloaded"
    );
    assert_eq!(f.manager.history(&id.to_string()).await.unwrap().len(), 1);
    peer.cx
        .send_request(CloseSessionRequest::new(id.clone()))
        .block_task()
        .await
        .unwrap();
    peer.cx
        .send_request(CloseSessionRequest::new("never-existed"))
        .block_task()
        .await
        .unwrap();
    let detached = peer
        .cx
        .send_request(PromptRequest::new(id.clone(), vec!["hello".into()]))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(detached.code.value(), -32002);
    peer.cx
        .send_request(ResumeSessionRequest::new(id.clone(), f.directory.path()))
        .block_task()
        .await
        .unwrap();
    peer.cx
        .send_request(PromptRequest::new(id, vec!["hello".into()]))
        .block_task()
        .await
        .unwrap();
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn close_is_not_advertised_or_called_when_an_adapter_cannot_release_resources() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(directory.path().join("codex")).unwrap();
    std::fs::write(directory.path().join("codex/no-close"), "").unwrap();
    let manager = Fixture::connect(directory.path()).await;
    let f = Fixture { directory, manager };
    let (peer, stop, task) = connected(&f).await;
    assert!(
        peer.initialize()
            .await
            .agent_capabilities
            .session_capabilities
            .close
            .is_none()
    );
    let error = peer
        .cx
        .send_request(CloseSessionRequest::new("anything"))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(error.code.value(), -32601);
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn native_prompt_errors_retain_acp_codes_and_data() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    peer.initialize().await;
    let id = peer.new_session(f.directory.path()).await;
    let error = peer
        .cx
        .send_request(PromptRequest::new(id, vec!["auth-error".into()]))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        agent_client_protocol::Error::auth_required().code
    );
    assert_eq!(error.data, Some(json!({"provider":"fixture"})));
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn delete_succeeds_when_the_workspace_has_moved() {
    let f = Fixture::new().await;
    let cwd = f.directory.path().join("project");
    std::fs::create_dir(&cwd).unwrap();
    let (peer, stop, task) = connected(&f).await;
    peer.initialize().await;
    let id = peer.new_session(&cwd).await;
    std::fs::remove_dir(&cwd).unwrap();
    peer.cx
        .send_request(DeleteSessionRequest::new(id.clone()))
        .block_task()
        .await
        .unwrap();
    std::fs::create_dir(&cwd).unwrap();
    assert!(matches!(
        f.manager.get(&id.to_string()).await,
        Err(tether::manager::Error::NotFound)
    ));
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn delete_fails_offline_and_retains_the_visible_session_and_history() {
    let f = Fixture::new().await;
    let session = f.create("codex", tether::model::SessionKind::Managed).await;
    f.send(&session.id, None, "hello").await;
    f.finished(&session.id, 1).await;
    f.send(&session.id, None, "crash").await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while f
            .manager
            .providers()
            .iter()
            .any(|p| p.id == "codex" && p.ready)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (peer, stop, task) = connected(&f).await;
    peer.initialize().await;
    let error = peer
        .cx
        .send_request(DeleteSessionRequest::new(session.id.clone()))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(error.code.value(), -32003);
    let list = peer
        .cx
        .send_request(ListSessionsRequest::new())
        .block_task()
        .await
        .unwrap();
    assert!(
        list.sessions
            .iter()
            .any(|s| s.session_id.to_string() == session.id)
    );
    assert!(serde_json::to_value(&list).unwrap()["_meta"]["tether"]["discoveryPerformed"] == false);
    assert!(!f.manager.history(&session.id).await.unwrap().is_empty());
    assert!(
        f.directory
            .path()
            .join("codex")
            .join(format!("{}.json", session.binding().session_id))
            .exists()
    );
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn closing_invalidates_other_clients_without_detaching_a_later_resume() {
    let f = Fixture::new().await;
    let (first, first_stop, first_task) = connected(&f).await;
    let (second, second_stop, second_task) = connected(&f).await;
    first.initialize().await;
    second.initialize().await;
    let id = first.new_session(f.directory.path()).await;
    second
        .cx
        .send_request(ResumeSessionRequest::new(id.clone(), f.directory.path()))
        .block_task()
        .await
        .unwrap();
    first
        .cx
        .send_request(CloseSessionRequest::new(id.clone()))
        .block_task()
        .await
        .unwrap();
    first
        .cx
        .send_request(ResumeSessionRequest::new(id.clone(), f.directory.path()))
        .block_task()
        .await
        .unwrap();
    let detached = second
        .cx
        .send_request(PromptRequest::new(id.clone(), vec!["hello".into()]))
        .block_task()
        .await
        .unwrap_err();
    assert_eq!(detached.code.value(), -32002);
    first
        .cx
        .send_request(PromptRequest::new(id, vec!["hello".into()]))
        .block_task()
        .await
        .unwrap();
    second.close().await;
    second_stop.cancel();
    let _ = second_task.await;
    close(f, first, first_stop, first_task).await;
}

#[tokio::test]
async fn session_list_exposes_standard_provider_activity_timestamp() {
    let f = Fixture::new().await;
    let directory = f.directory.path().join("codex");
    std::fs::write(
        directory.join("external.json"),
        serde_json::to_vec(&json!({
            "sessionId":"external", "cwd":f.directory.path(), "title":"External chat",
            "updatedAt":"2026-11-10T10:00:00Z"
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(directory.join("external.history"), "").unwrap();
    f.manager.discover_and_list(None, None).await.unwrap();
    let (peer, stop, task) = connected(&f).await;
    peer.initialize().await;
    let page = peer
        .cx
        .send_request(ListSessionsRequest::new())
        .block_task()
        .await
        .unwrap();
    assert_eq!(page.sessions.len(), 1);
    assert_eq!(
        page.sessions[0].updated_at.as_deref(),
        Some("2026-11-10T10:00:00Z")
    );
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn native_discovery_is_advertised_and_requires_explicit_list_metadata() {
    let f = Fixture::new().await;
    std::fs::write(
        f.directory.path().join("codex/external.json"),
        serde_json::to_vec(&json!({
            "sessionId":"external", "cwd":f.directory.path(), "title":"External"
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(f.directory.path().join("codex/external.history"), "").unwrap();
    let (peer, stop, task) = connected(&f).await;
    let init = serde_json::to_value(peer.initialize().await).unwrap();
    assert_eq!(
        init["agentCapabilities"]["_meta"]["tether"]["sessionDiscovery"],
        true
    );
    let known = peer
        .cx
        .send_request(ListSessionsRequest::new())
        .block_task()
        .await
        .unwrap();
    assert!(known.sessions.is_empty());
    let discovery = peer
        .cx
        .send_request(
            ListSessionsRequest::new().meta(
                [("tether".into(), json!({"discover":true}))]
                    .into_iter()
                    .collect::<Meta>(),
            ),
        )
        .block_task()
        .await
        .unwrap();
    assert_eq!(discovery.sessions.len(), 1);
    assert_eq!(
        serde_json::to_value(&discovery).unwrap()["_meta"]["tether"]["discoveryPerformed"],
        true
    );
    let known = peer
        .cx
        .send_request(ListSessionsRequest::new())
        .block_task()
        .await
        .unwrap();
    assert_eq!(
        known.sessions[0].session_id,
        discovery.sessions[0].session_id
    );
    assert_eq!(
        serde_json::to_value(&known).unwrap()["_meta"]["tether"]["discoveryPerformed"],
        false
    );
    close(f, peer, stop, task).await;
}

#[tokio::test]
async fn steering_and_queue_extensions_deliver_fifo_after_the_original_client_disconnects() {
    let f = Fixture::new().await;
    let (peer, stop, task) = connected(&f).await;
    let init = peer.initialize().await;
    assert_eq!(init.meta.as_ref().unwrap()["steering"]["supported"], true);
    assert_eq!(
        init.agent_capabilities.meta.as_ref().unwrap()["tether"]["messageQueue"],
        true
    );
    let id = peer.new_session(f.directory.path()).await;
    let pending = tokio::spawn(
        peer.cx
            .send_request(PromptRequest::new(id.clone(), vec!["slow original".into()]))
            .block_task(),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.manager.get(&id.to_string()).await.unwrap().status != "running" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let queued=peer.cx.send_request(UntypedMessage::new("_tether/session/queue",json!({"sessionId":id,"messageId":"q","prompt":[{"type":"text","text":"queued after disconnect"}]})).unwrap()).block_task().await.unwrap();
    assert_eq!(queued["state"], "queued");
    peer.close().await;
    stop.cancel();
    let _ = task.await;
    let _ = pending.await;
    let (reader, reader_stop, reader_task) = connected(&f).await;
    reader.initialize().await;
    reader
        .cx
        .send_request(ResumeSessionRequest::new(id.clone(), f.directory.path()))
        .block_task()
        .await
        .unwrap();
    let result = reader
        .cx
        .send_request(
            UntypedMessage::new(
                "_session/steering",
                json!({"sessionId":id,"prompt":[{"type":"text","text":"finish original"}]}),
            )
            .unwrap(),
        )
        .block_task()
        .await
        .unwrap();
    assert_eq!(result["outcome"], "injected");
    f.finished(&id.to_string(), 2).await;
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            if reader.updates.lock().unwrap().iter().any(|u| matches!(&u.update,SessionUpdate::UserMessageChunk(c) if matches!(&c.content,ContentBlock::Text(t) if t.text=="queued after disconnect"))) { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let listed = reader
        .cx
        .send_request(
            UntypedMessage::new("_tether/session/queue/list", json!({"sessionId":id})).unwrap(),
        )
        .block_task()
        .await
        .unwrap();
    assert_eq!(listed["messages"], json!([]));
    close(f, reader, reader_stop, reader_task).await;
}
