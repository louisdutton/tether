//! Stable ACP v1 application boundary. Wire framing and JSON-RPC belong to the SDK.
use crate::{
    manager::{self, Manager},
    model,
};
use agent_client_protocol::v1::{ConnectionTo, schema::*};
use agent_client_protocol::{Agent, Client, ConnectTo, Error, UntypedMessage};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<T, Error>;

#[derive(Default)]
struct State {
    initialized: bool,
    extensions: bool,
    attached: HashMap<String, u64>,
    cwd: HashMap<String, std::path::PathBuf>,
    active: HashMap<String, Arc<Active>>,
    owned_turns: HashSet<(String, String)>,
    output: Output,
}
#[derive(Default)]
struct Active {
    cancel: CancellationToken,
    turn: Mutex<Option<String>>,
}
#[derive(Default)]
struct Output {
    text: HashMap<String, String>,
    tools: HashSet<String>,
    snapshots: HashSet<String>,
    updates: HashSet<String>,
}

fn params<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|e| Error::invalid_params().data(e.to_string()))
}
fn value<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(|_| Error::internal_error())
}
fn mapped(error: manager::Error) -> Error {
    match error {
        manager::Error::Invalid(message) => Error::invalid_params().data(message),
        manager::Error::NotFound => Error::new(-32002, "Session or turn not found"),
        manager::Error::Deletion(message) => {
            Error::new(-32003, "Permanent deletion failed").data(message)
        }
        manager::Error::Busy => Error::new(-32001, "Session already has an active turn"),
        manager::Error::Provider(error) if error.downcast_ref::<Error>().is_some() => {
            error.downcast::<Error>().expect("checked ACP error type")
        }
        other => {
            tracing::warn!(?other, "ACP operation failed");
            Error::internal_error().data("agent or storage operation failed; inspect server logs")
        }
    }
}
fn options(meta: Option<&Meta>) -> Result<Options> {
    meta.and_then(|m| m.get("tether"))
        .cloned()
        .map(params)
        .transpose()
        .map(|o| o.unwrap_or_default())
}
#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Options {
    kind: Option<model::SessionKind>,
}
fn metadata(session: &model::Session) -> Meta {
    [ ("tether".into(), json!({"provider":session.provider,"kind":session.kind,"machineId":session.machine_id,"status":session.status,"bindings":session.bindings,"deletionPending":session.deletion_pending,"deletionError":session.deletion_error})) ].into_iter().collect()
}
fn attached(manager: &Manager, state: &Mutex<State>, id: &str) -> Result<()> {
    if state.lock().unwrap().attached.get(id).copied() != Some(manager.session_epoch(id)) {
        return Err(Error::new(
            -32002,
            "Session is not attached; load or create it on this connection first",
        ));
    }
    Ok(())
}
fn setup(
    cwd: &std::path::Path,
    directories: &[std::path::PathBuf],
    system: bool,
    mcp: &[McpServer],
) -> Result<()> {
    if !cwd.is_absolute() {
        return Err(Error::invalid_params().data("cwd must be absolute"));
    }
    if !directories.is_empty() || system {
        return Err(Error::invalid_params()
            .data("additional directories and system-prompt overrides are not supported"));
    }
    crate::providers::validate_mcp(mcp).map_err(|e| Error::invalid_params().data(e.to_string()))?;
    Ok(())
}

/// Serve one independent ACP connection over a supported SDK transport.
pub async fn serve(
    transport: impl ConnectTo<Agent>,
    manager: Arc<Manager>,
    shutdown: CancellationToken,
) -> Result<()> {
    let state = Arc::new(Mutex::new(State::default()));
    let request_state = state.clone();
    let request_manager = manager.clone();
    let notification_state = state.clone();
    let notification_manager = manager.clone();
    let mut events = manager.subscribe();
    Agent.v1().name("tether")
        .on_receive_request(async move |request: UntypedMessage, responder, cx| {
            let (method, payload) = request.into_parts();
            if method == "session/prompt" {
                let result = (|| {
                    if !request_state.lock().unwrap().initialized { return Err(Error::invalid_request().data("initialize first")); }
                    let request: PromptRequest = params(payload)?;
                    let id = request.session_id.to_string();
                    attached(&request_manager, &request_state, &id)?;
                    let mut state = request_state.lock().unwrap();
                    if state.active.contains_key(&id) { return Err(mapped(manager::Error::Busy)); }
                    let active = Arc::new(Active::default());
                    state.active.insert(id, active.clone());
                    Ok((request, active))
                })();
                return match result {
                    Err(error) => responder.respond_with_error(error),
                    Ok((request, active)) => {
                        let manager = request_manager.clone();
                        let state = request_state.clone();
                        let connection = cx.clone();
                        cx.spawn(async move {
                            let id = request.session_id.to_string();
                            let result = prompt(&manager, &state, &connection, request, active).await;
                            state.lock().unwrap().active.remove(&id);
                            responder.respond_with_result(result)
                        })
                    }
                };
            }
            let result = handle_request(&request_manager, &request_state, &cx, &method, payload).await;
            responder.respond_with_result(result)
        }, agent_client_protocol::on_receive_request!())
        .on_receive_notification(async move |notification: UntypedMessage, _cx| {
            if notification.method() == "session/cancel" {
                let request: CancelNotification = params(notification.params().clone())?;
                let id = request.session_id.to_string();
                if attached(&notification_manager, &notification_state, &id).is_ok() {
                    let active = notification_state.lock().unwrap().active.get(&id).cloned();
                    if let Some(active) = active { active.cancel.cancel(); }
                    else if let Ok(turns) = notification_manager.history(&id).await
                        && let Some(turn) = turns.iter().rev().find(|t| t.content.status == "inProgress") {
                            let _ = notification_manager.interrupt(&id, &turn.id).await;
                        }
                }
            }
            Ok(())
        }, agent_client_protocol::on_receive_notification!())
        .connect_with(transport, async move |cx| {
            loop {
                let event = tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = cx.incoming_closed() => return Ok(()),
                    event = events.recv() => match event {
                        Ok(event) => event,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            if state.lock().unwrap().extensions { extension(&cx, json!({"kind":"resync.required"}))?; }
                            continue;
                        }
                        Err(_) => return Ok(()),
                    }
                };
                let Some(id) = &event.session_id else { continue };
                let extensions = state.lock().unwrap().extensions;
                if extensions && matches!(event.kind.as_str(), "client.requested" | "client.resolved") {
                    extension(&cx, value(&event)?)?;
                    continue;
                }
                if extensions && (matches!(event.kind.as_str(), "turn.started" | "turn.completed" | "turn.interrupted" | "turn.failed" | "provider.unavailable" | "session.deleted" | "session.closed") ||
                    (event.kind == "acp.session_update" && event.data["sessionUpdate"] == "session_info_update")) {
                    if let Ok(session) = manager.get(id).await {
                        extension(&cx, json!({"kind":"session.status","session_id":id,"turn_id":event.turn_id,
                            "data":{"status":session.status,"cwd":session.cwd,"title":session.title,"updatedAt":session.updated_at,"outcome":event.kind,"responsePreview":event.data["responsePreview"]}}))?;
                    } else if event.kind == "session.deleted" {
                        extension(&cx, json!({"kind":"session.deleted","session_id":id}))?;
                    }
                }
                if matches!(event.kind.as_str(), "session.deleted" | "session.closed") {
                    let mut state = state.lock().unwrap();
                    // A delayed close event must not detach a newer resume/load.
                    let epoch = event.data["epoch"].as_u64().unwrap_or(0);
                    if state.attached.get(id).is_some_and(|attached| *attached >= epoch) { continue; }
                    state.attached.remove(id);
                    state.cwd.remove(id);
                    state.output.clear_session(id);
                    state.owned_turns.retain(|(session, _)| session != id);
                    if let Some(active) = state.active.get(id) { active.cancel.cancel(); }
                    continue;
                }
                let forward = {
                    let state = state.lock().unwrap();
                    state.attached.get(id).copied() == Some(manager.session_epoch(id))
                        && (event.kind == "queue.updated" || !state.active.get(id).is_some_and(|active| {
                            let turn = active.turn.lock().unwrap();
                            turn.is_none() || event.turn_id.is_none() || *turn == event.turn_id
                        }))
                        && !event.turn_id.as_ref().is_some_and(|turn| state.owned_turns.contains(&(id.clone(), turn.clone())))
                };
                if forward { forward_event(&state, &cx, &event)?; }
            }
        }).await
}

async fn handle_request(
    manager: &Manager,
    state: &Mutex<State>,
    cx: &ConnectionTo<Client>,
    method: &str,
    payload: Value,
) -> Result<Value> {
    if method == "initialize" {
        let request: InitializeRequest = params(payload)?;
        let mut state = state.lock().unwrap();
        if state.initialized {
            return Err(Error::invalid_request().data("connection already initialized"));
        }
        state.initialized = true;
        state.extensions = request
            .client_capabilities
            .meta
            .as_ref()
            .is_some_and(|m| m.get("tether").is_some());
        let capabilities = AgentCapabilities::new().load_session(true).prompt_capabilities(manager.prompt_capabilities()).mcp_capabilities(manager.mcp_capabilities())
            .session_capabilities(SessionCapabilities::new()
                .list(SessionListCapabilities::new())
                .delete(SessionDeleteCapabilities::new())
                .resume(SessionResumeCapabilities::new())
                .close(manager.supports_close().then(SessionCloseCapabilities::new)))
            .meta([("tether".into(), json!({"version":1,"providers":true,"managedHistory":true,"agentSwitching":true,"directSessions":true,"sessionDiscovery":true,"messageQueue":true,"sessionStatus":true,"clientRequests":true,"eventReplay":true}))].into_iter().collect::<Meta>());
        return value(
            InitializeResponse::new(agent_client_protocol::schema::ProtocolVersion::V1)
                .agent_info(Implementation::new("tether", env!("CARGO_PKG_VERSION")))
                .agent_capabilities(capabilities)
                .meta(
                    [(
                        "steering".into(),
                        json!({"supported":manager.supports_steering()}),
                    )]
                    .into_iter()
                    .collect::<Meta>(),
                ),
        );
    }
    if !state.lock().unwrap().initialized {
        return Err(Error::invalid_request().data("initialize first"));
    }
    match method {
        "_tether/events/list" => {
            let cursor = payload
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::parse::<u64>)
                .transpose()
                .map_err(|_| Error::invalid_params().data("invalid monitoring cursor"))?;
            value(manager.monitor_events(cursor).map_err(mapped)?)
        }
        "_tether/requests/list" => {
            value(json!({"requests":manager.client_requests().map_err(mapped)?}))
        }
        "_tether/requests/respond" => {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct Answer {
                session_id: String,
                request_id: String,
                response: Value,
            }
            let request: Answer = params(payload)?;
            manager
                .answer_request(&request.session_id, &request.request_id, request.response)
                .map_err(mapped)?;
            value(json!({}))
        }
        "_session/steering"
        | "_tether/session/queue"
        | "_tether/session/queue/list"
        | "_tether/session/queue/delete"
        | "_tether/session/queue/resume" => {
            let id = payload["sessionId"]
                .as_str()
                .ok_or_else(Error::invalid_params)?;
            attached(manager, state, id)?;
            if method == "_tether/session/queue/list" {
                return Ok(json!({"messages":manager.queued_messages(id).map_err(mapped)?}));
            }
            if matches!(
                method,
                "_tether/session/queue/delete" | "_tether/session/queue/resume"
            ) {
                let message = payload["messageId"]
                    .as_str()
                    .ok_or_else(Error::invalid_params)?;
                manager
                    .change_queued_message(id, message, method.ends_with("/resume"))
                    .await
                    .map_err(mapped)?;
                return Ok(json!({"messages":manager.queued_messages(id).map_err(mapped)?}));
            }
            let prompt: Vec<ContentBlock> = params(payload["prompt"].clone())?;
            if method == "_session/steering" {
                return manager.steer(id, prompt).await.map_err(mapped);
            }
            let message = payload["messageId"]
                .as_str()
                .ok_or_else(Error::invalid_params)?
                .to_owned();
            value(manager.enqueue(id, message, prompt).await.map_err(mapped)?)
        }
        "session/new" => {
            let request: NewSessionRequest = params(payload)?;
            setup(
                &request.cwd,
                &request.additional_directories,
                request.system_prompt.is_some(),
                &request.mcp_servers,
            )?;
            let opts = options(request.meta.as_ref())?;
            let session = manager
                .create(model::CreateSession {
                    cwd: request.cwd.to_string_lossy().into(),
                    provider: manager.default_provider().into(),
                    kind: opts.kind.unwrap_or_default(),
                    mcp_servers: request.mcp_servers,
                })
                .await
                .map_err(mapped)?;
            {
                let mut state = state.lock().unwrap();
                state
                    .attached
                    .insert(session.id.clone(), manager.session_epoch(&session.id));
                state
                    .cwd
                    .insert(session.id.clone(), session.cwd.clone().into());
            }
            for update in manager.session_updates(&session.id).await.map_err(mapped)? {
                notify(cx, &session.id, update)?;
            }
            value(
                NewSessionResponse::new(session.id.clone())
                    .modes(manager.modes(&session.id).await.map_err(mapped)?)
                    .config_options(manager.config(&session.id).await.map_err(mapped)?)
                    .meta(metadata(&session)),
            )
        }
        "session/load" => {
            let request: LoadSessionRequest = params(payload)?;
            setup(
                &request.cwd,
                &request.additional_directories,
                request.system_prompt.is_some(),
                &request.mcp_servers,
            )?;
            let id = request.session_id.to_string();
            if state.lock().unwrap().active.contains_key(&id) {
                return Err(mapped(manager::Error::Busy));
            }
            let session = manager
                .load(&id, &request.cwd, request.mcp_servers)
                .await
                .map_err(mapped)?;
            {
                let mut state = state.lock().unwrap();
                state.attached.remove(&id);
                state.cwd.insert(id.clone(), session.cwd.clone().into());
            }
            let history = manager.history(&id).await.map_err(mapped)?;
            state.lock().unwrap().output.clear_session(&id);
            for turn in history {
                replay(state, cx, &id, &turn)?;
            }
            state
                .lock()
                .unwrap()
                .attached
                .insert(id.clone(), manager.session_epoch(&id));
            // Reconcile work that finished between the first snapshot and attachment.
            for turn in manager.history(&id).await.map_err(mapped)? {
                replay(state, cx, &id, &turn)?;
            }
            for update in manager.session_updates(&id).await.map_err(mapped)? {
                notify(cx, &id, update)?;
            }
            value(
                LoadSessionResponse::new()
                    .modes(manager.modes(&session.id).await.map_err(mapped)?)
                    .config_options(manager.config(&session.id).await.map_err(mapped)?)
                    .meta(metadata(&session)),
            )
        }
        "authenticate" => {
            let _: AuthenticateRequest = params(payload)?;
            Err(Error::invalid_params().data("no authentication methods are advertised; authenticate the provider on the execution host"))
        }
        "session/resume" => {
            let request: ResumeSessionRequest = params(payload)?;
            setup(
                &request.cwd,
                &request.additional_directories,
                request.system_prompt.is_some(),
                &request.mcp_servers,
            )?;
            let id = request.session_id.to_string();
            let session = manager
                .load(&id, &request.cwd, request.mcp_servers)
                .await
                .map_err(mapped)?;
            if session.status == "unavailable" {
                return Err(
                    Error::internal_error().data("provider unavailable; cannot resume execution")
                );
            }
            {
                let mut state = state.lock().unwrap();
                state
                    .attached
                    .insert(id.clone(), manager.session_epoch(&id));
                state.cwd.insert(id, session.cwd.clone().into());
            }
            value(
                ResumeSessionResponse::new()
                    .modes(manager.modes(&session.id).await.map_err(mapped)?)
                    .config_options(manager.config(&session.id).await.map_err(mapped)?)
                    .meta(metadata(&session)),
            )
        }
        "session/delete" => {
            let request: DeleteSessionRequest = params(payload)?;
            let id = request.session_id.to_string();
            if state.lock().unwrap().active.contains_key(&id) {
                return Err(mapped(manager::Error::Busy));
            }
            manager.delete(&id).await.map_err(mapped)?;
            let mut state = state.lock().unwrap();
            state.attached.remove(&id);
            state.output.clear_session(&id);
            value(DeleteSessionResponse::new())
        }
        "session/close" => {
            if !manager.supports_close() {
                return Err(Error::method_not_found());
            }
            let request: CloseSessionRequest = params(payload)?;
            let id = request.session_id.to_string();
            manager.close(&id).await.map_err(mapped)?;
            let mut state = state.lock().unwrap();
            state.attached.remove(&id);
            state.output.clear_session(&id);
            if let Some(active) = state.active.get(&id) {
                active.cancel.cancel();
            }
            value(CloseSessionResponse::new())
        }
        "session/set_mode" => {
            let request: SetSessionModeRequest = params(payload)?;
            let id = request.session_id.to_string();
            attached(manager, state, &id)?;
            manager
                .set_mode(&id, request.mode_id)
                .await
                .map_err(mapped)?;
            value(SetSessionModeResponse::new())
        }
        "session/list" => {
            let request: ListSessionsRequest = params(payload)?;
            if request.cwd.as_ref().is_some_and(|p| !p.is_absolute()) {
                return Err(Error::invalid_params().data("cwd must be absolute"));
            }
            let discover = request
                .meta
                .as_ref()
                .and_then(|m| m.get("tether"))
                .and_then(|m| m.get("discover"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let page = if discover {
                manager
                    .discover_and_list(request.cursor, request.cwd.as_deref())
                    .await
            } else {
                manager
                    .list_filtered(request.cursor, request.cwd.as_deref())
                    .await
            }
            .map_err(mapped)?;
            let sessions = page
                .data
                .into_iter()
                .map(|s| {
                    SessionInfo::new(s.id.clone(), &s.cwd)
                        .title(s.title.clone())
                        .updated_at(s.updated_at.clone())
                        .meta(metadata(&s))
                })
                .collect();
            value(ListSessionsResponse::new(sessions).next_cursor(page.next_cursor).meta(
                [("tether".into(), json!({"catalogComplete":page.failures.is_empty(),"discoveryPerformed":discover,"catalogErrors":page.failures}))].into_iter().collect::<Meta>()
            ))
        }
        "session/set_config_option" => {
            let request: SetSessionConfigOptionRequest = params(payload)?;
            let id = request.session_id.to_string();
            attached(manager, state, &id)?;
            let options = manager
                .set_config(&id, &request.config_id.to_string(), request.value)
                .await
                .map_err(mapped)?;
            value(SetSessionConfigOptionResponse::new(options))
        }
        "_tether/providers" => value(json!({"providers":manager.providers()})),
        "_tether/session/import" => {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Id {
                session_id: String,
            }
            let request: Id = params(payload)?;
            value(manager.import(&request.session_id).await.map_err(mapped)?)
        }
        "_tether/session/adopt" => {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Adopt {
                provider: String,
                provider_session_id: String,
                #[serde(default)]
                kind: model::SessionKind,
            }
            let request: Adopt = params(payload)?;
            let session = manager
                .adopt(model::AdoptSession {
                    provider: request.provider,
                    provider_session_id: request.provider_session_id,
                    kind: request.kind,
                })
                .await
                .map_err(mapped)?;
            // Adopt supplies metadata; load is still required to subscribe and replay.
            value(session)
        }
        _ => Err(Error::method_not_found()),
    }
}

async fn prompt(
    manager: &Manager,
    state: &Mutex<State>,
    cx: &ConnectionTo<Client>,
    request: PromptRequest,
    active: Arc<Active>,
) -> Result<Value> {
    let id = request.session_id.to_string();
    if request
        .meta
        .as_ref()
        .is_some_and(|m| m.contains_key("tether"))
    {
        return Err(Error::invalid_params()
            .data("use session/set_config_option to select an agent or model"));
    }
    let content = request.prompt;
    let text = content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(t) => Some(t.text.clone()),
            ContentBlock::ResourceLink(r) => {
                Some(format!("Resource reference: {} ({})", r.uri, r.name))
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let mut events = manager.subscribe();
    if active.cancel.is_cancelled() {
        return value(PromptResponse::new(StopReason::Cancelled));
    }
    let turn = match manager
        .message_content(
            &id,
            model::Message {
                provider: None,
                text,
            },
            content,
        )
        .await
    {
        Ok(turn) => turn,
        Err(_) if active.cancel.is_cancelled() => {
            return value(PromptResponse::new(StopReason::Cancelled));
        }
        Err(error) => return Err(mapped(error)),
    };
    // The connection-wide observer can lag behind this prompt's private reader.
    // Keep ownership after completion so it cannot re-send buffered turn output.
    state
        .lock()
        .unwrap()
        .owned_turns
        .insert((id.clone(), turn.clone()));
    *active.turn.lock().unwrap() = Some(turn.clone());
    let mut cancelled = false;
    loop {
        let event = tokio::select! {
            _ = active.cancel.cancelled(), if !cancelled => {
                cancelled = true;
                // It may have completed before the cancellation reached its provider.
                if let Err(error) = manager.interrupt(&id, &turn).await { tracing::debug!(?error, "ACP cancellation raced provider completion"); }
                continue;
            }
            event = events.recv() => match event {
                Ok(event) => event,
                Err(_) => return Err(Error::internal_error().data("event stream lost; reload the session to reconcile history")),
            }
        };
        if event.kind == "resync.required" && event.session_id.is_none() {
            return Err(
                Error::internal_error().data("event persistence failed; reload session history")
            );
        }
        if event.session_id.as_deref() != Some(&id) {
            continue;
        }
        if event.kind == "provider.unavailable" || event.kind == "resync.required" {
            return Err(
                Error::internal_error().data("provider unavailable; reload session history")
            );
        }
        if event.turn_id.as_deref().is_some_and(|t| t != turn) {
            continue;
        }
        if event.kind != "queue.updated" {
            forward_event(state, cx, &event)?;
        }
        match event.kind.as_str() {
            "turn.completed" | "turn.interrupted" => {
                let mut response = serde_json::from_value::<PromptResponse>(event.data.clone())
                    .unwrap_or_else(|_| PromptResponse::new(StopReason::EndTurn));
                if cancelled || active.cancel.is_cancelled() || event.kind == "turn.interrupted" {
                    response.stop_reason = StopReason::Cancelled;
                }
                return value(response);
            }
            "turn.failed" if cancelled || active.cancel.is_cancelled() => {
                return value(PromptResponse::new(StopReason::Cancelled));
            }
            "turn.failed" => {
                if let Some(error) = event.data.get("error")
                    && let Ok(error) = serde_json::from_value::<Error>(error.clone())
                {
                    return Err(error);
                }
                return Err(Error::internal_error()
                    .data(json!({"message":"agent turn failed","sessionId":id,"turnId":turn})));
            }
            _ => {}
        }
    }
}

fn extension(cx: &ConnectionTo<Client>, event: Value) -> Result<()> {
    cx.send_notification(UntypedMessage::new("_tether/event", event)?)
}
fn notify(cx: &ConnectionTo<Client>, id: &str, update: SessionUpdate) -> Result<()> {
    cx.send_notification(SessionNotification::new(id.to_owned(), update))
}
fn replay(
    state: &Mutex<State>,
    cx: &ConnectionTo<Client>,
    id: &str,
    turn: &model::TranscriptTurn,
) -> Result<()> {
    for item in &turn.content.items {
        let event = model::Event {
            session_id: Some(id.into()),
            turn_id: Some(turn.id.clone()),
            kind: item.kind.clone(),
            data: serde_json::to_value(item).map_err(|_| Error::internal_error())?,
        };
        forward_event(state, cx, &event)?;
        // A replay snapshot can be ahead of buffered live deltas. Wait for the
        // completed item to append its remaining suffix instead of duplicating text.
        if matches!(item.kind.as_str(), "message.assistant" | "message.user") {
            state
                .lock()
                .unwrap()
                .output
                .snapshots
                .insert(format!("{id}/{}/{}", turn.id, item.id));
        }
    }
    Ok(())
}
impl Output {
    fn clear_session(&mut self, id: &str) {
        let prefix = format!("{id}/");
        self.text.retain(|k, _| !k.starts_with(&prefix));
        self.tools.retain(|k| !k.starts_with(&prefix));
        self.snapshots.retain(|k| !k.starts_with(&prefix));
        self.updates.retain(|k| !k.starts_with(&prefix));
    }
}
fn forward_event(
    state: &Mutex<State>,
    cx: &ConnectionTo<Client>,
    event: &model::Event,
) -> Result<()> {
    let Some(id) = &event.session_id else {
        return Ok(());
    };
    let turn = event.turn_id.as_deref().unwrap_or("session");
    let mut state = state.lock().unwrap();
    match event.kind.as_str() {
        "acp.update" => {
            let item: model::Item = params(event.data.clone())?;
            if !state
                .output
                .updates
                .insert(format!("{id}/{turn}/{}", item.id))
            {
                return Ok(());
            }
            let mut update = item.provider_data;
            // Native identifiers are scoped to their binding. Tether's turn ID
            // makes them unique across provider changes and history replay.
            for field in ["messageId", "toolCallId"] {
                if let Some(native) = update.get(field).and_then(Value::as_str) {
                    update[field] = Value::String(format!("{turn}/{native}"));
                }
            }
            let mut update: SessionUpdate = params(update)?;
            if let Some(cwd) = state.cwd.get(id) {
                crate::providers::absolute_update_paths(&mut update, cwd);
            }
            notify(cx, id, update)?;
        }
        "acp.session_update" => notify(cx, id, params(event.data.clone())?)?,
        "session.agent_changed" => {
            notify(
                cx,
                id,
                SessionUpdate::SessionInfoUpdate(
                    SessionInfoUpdate::new().meta(
                        [("tether".into(), json!({"provider":event.data["to"]}))]
                            .into_iter()
                            .collect::<Meta>(),
                    ),
                ),
            )?;
        }
        "message.delta" => {
            if let (Some(item), Some(text)) =
                (event.data["item_id"].as_str(), event.data["text"].as_str())
            {
                let message = format!("{turn}/{item}");
                if state.output.snapshots.contains(&format!("{id}/{message}")) {
                    return Ok(());
                }
                state
                    .output
                    .text
                    .entry(format!("{id}/{message}"))
                    .or_default()
                    .push_str(text);
                notify(
                    cx,
                    id,
                    SessionUpdate::AgentMessageChunk(
                        ContentChunk::new(text.to_owned().into())
                            .message_id(MessageId::new(message)),
                    ),
                )?;
            }
        }
        "message.assistant" | "message.user" => {
            let item: model::Item = params(event.data.clone())?;
            if event.kind == "message.user" && !item.provider_data.is_null() {
                if state
                    .output
                    .updates
                    .insert(format!("{id}/{turn}/{}", item.id))
                {
                    notify(
                        cx,
                        id,
                        SessionUpdate::UserMessageChunk(
                            ContentChunk::new(params::<ContentBlock>(item.provider_data)?)
                                .message_id(MessageId::new(format!("{turn}/{}", item.id))),
                        ),
                    )?;
                }
                return Ok(());
            }
            if let Some(text) = item.text {
                let message = format!("{turn}/{}", item.id);
                let sent = state
                    .output
                    .text
                    .entry(format!("{id}/{message}"))
                    .or_default();
                let suffix = text.strip_prefix(sent.as_str()).unwrap_or("").to_owned();
                *sent = text;
                if !suffix.is_empty() {
                    let chunk =
                        ContentChunk::new(suffix.into()).message_id(MessageId::new(message));
                    notify(
                        cx,
                        id,
                        if event.kind == "message.user" {
                            SessionUpdate::UserMessageChunk(chunk)
                        } else {
                            SessionUpdate::AgentMessageChunk(chunk)
                        },
                    )?;
                }
            }
        }
        "tool.activity" => {
            let item: model::Item = params(event.data.clone())?;
            let raw = &item.provider_data;
            let native_id = raw["tool_use_id"]
                .as_str()
                .or_else(|| raw["id"].as_str())
                .unwrap_or(&item.id);
            let tool = format!("{turn}/{native_id}");
            let status = if raw["is_error"] == true || raw["status"] == "failed" {
                ToolCallStatus::Failed
            } else if raw["type"] == "tool_use" {
                ToolCallStatus::InProgress
            } else {
                ToolCallStatus::Completed
            };
            let title = raw["name"]
                .as_str()
                .or_else(|| raw["command"].as_str())
                .unwrap_or("Agent tool activity");
            if state.output.tools.insert(format!("{id}/{tool}")) {
                notify(
                    cx,
                    id,
                    SessionUpdate::ToolCall(
                        ToolCall::new(tool, title)
                            .status(status)
                            .raw_output(item.provider_data),
                    ),
                )?;
            } else {
                notify(
                    cx,
                    id,
                    SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                        tool,
                        ToolCallUpdateFields::new()
                            .status(status)
                            .raw_output(item.provider_data),
                    )),
                )?;
            }
        }
        _ if state.extensions => extension(cx, value(event)?)?,
        _ => {}
    }
    Ok(())
}

/// Standard ACP newline-delimited stdio transport, with a bounded input line size.
pub async fn stdio(manager: Arc<Manager>, shutdown: CancellationToken) -> Result<()> {
    use futures::{SinkExt, TryStreamExt};
    use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};
    let input = FramedRead::new(
        tokio::io::stdin(),
        LinesCodec::new_with_max_length(64 * 1024),
    )
    .map_err(std::io::Error::other);
    let output = FramedWrite::new(tokio::io::stdout(), LinesCodec::new())
        .with(|line: String| {
            futures::future::ready(Ok::<_, tokio_util::codec::LinesCodecError>(line))
        })
        .sink_map_err(std::io::Error::other);
    serve(
        agent_client_protocol::Lines::new(output, input),
        manager,
        shutdown,
    )
    .await
}
