//! One ACP client implementation for every external agent. Native translation belongs
//! to the upstream adapter, not to Tether.
use super::Provider;
use crate::model::*;
use agent_client_protocol::{
    Agent, Client, Lines, UntypedMessage,
    v1::{ConnectionTo, schema::*},
};
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use futures::{SinkExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    process::Command,
    sync::{broadcast, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    codec::{FramedRead, FramedWrite, LinesCodec},
    sync::CancellationToken,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCommand {
    pub id: String,
    pub command: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default, rename = "permanentDelete")]
    pub permanent_delete: Option<DeleteCommand>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteCommand {
    #[serde(default)]
    pub command: Option<PathBuf>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Record {
    session: ProviderSession,
    #[serde(default)]
    config: Vec<SessionConfigOption>,
    #[serde(default)]
    modes: Option<SessionModeState>,
    #[serde(default)]
    selected: BTreeMap<String, SessionConfigOptionValue>,
    #[serde(default)]
    mcp: Vec<McpServer>,
    #[serde(default)]
    direct: bool,
    #[serde(skip)]
    loaded: bool,
    #[serde(skip)]
    loading: bool,
    #[serde(skip)]
    current: Option<String>,
    #[serde(skip)]
    turns: Vec<Turn>,
    #[serde(skip)]
    replay: Vec<Item>,
    #[serde(skip)]
    updates: BTreeMap<String, SessionUpdate>,
}
struct Shared {
    records: Mutex<BTreeMap<String, Record>>,
    pending: Mutex<Vec<SessionNotification>>,
    events: broadcast::Sender<Event>,
    ready: AtomicBool,
    path: PathBuf,
}
impl Shared {
    fn save(&self) -> Result<()> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let records = self.records.lock().unwrap();
        let tmp = self.path.with_extension("tmp");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)?;
        serde_json::to_writer(&mut file, &*records)?;
        file.flush()?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }
    fn emit(&self, session: &str, turn: Option<&str>, kind: &str, data: Value) {
        let _ = self.events.send(Event {
            session_id: Some(session.into()),
            turn_id: turn.map(str::to_owned),
            kind: kind.into(),
            data,
        });
    }
    fn update(&self, notification: SessionNotification) -> Result<()> {
        let id = notification.session_id.to_string();
        let mut records = self.records.lock().unwrap();
        let Some(record) = records.get_mut(&id) else {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() < 128 {
                pending.push(notification);
            }
            return Ok(());
        };
        let mut update = notification.update;
        super::absolute_update_paths(&mut update, Path::new(&record.session.cwd));
        match &update {
            SessionUpdate::CurrentModeUpdate(mode) => {
                if let Some(modes) = &mut record.modes {
                    modes.current_mode_id = mode.current_mode_id.clone();
                }
            }
            SessionUpdate::ConfigOptionUpdate(config) => {
                record.config = config.config_options.clone()
            }
            SessionUpdate::SessionInfoUpdate(info) => {
                // The SDK preserves omitted versus cleared fields; use the wire form.
                let info = serde_json::to_value(info)?;
                if let Some(title) = info.get("title") {
                    record.session.title = title.as_str().map(str::to_owned);
                }
            }
            _ => {}
        }
        if matches!(
            update,
            SessionUpdate::ConfigOptionUpdate(_)
                | SessionUpdate::SessionInfoUpdate(_)
                | SessionUpdate::AvailableCommandsUpdate(_)
                | SessionUpdate::CurrentModeUpdate(_)
        ) {
            if matches!(
                update,
                SessionUpdate::AvailableCommandsUpdate(_) | SessionUpdate::CurrentModeUpdate(_)
            ) {
                let kind = serde_json::to_value(&update)?["sessionUpdate"]
                    .as_str()
                    .unwrap()
                    .to_owned();
                record.updates.insert(kind, update.clone());
            }
            self.emit(
                &id,
                None,
                "acp.session_update",
                serde_json::to_value(update)?,
            );
        } else if record.loading {
            let item = raw_item(record.replay.len(), update)?;
            record.replay.push(item);
        } else if let Some(turn_id) = record.current.clone() {
            // Tether records authored input itself, so native echoes must not expose
            // injected handover context or duplicate the user's message.
            if matches!(update, SessionUpdate::UserMessageChunk(_)) {
                return Ok(());
            }
            let turn = record
                .turns
                .iter_mut()
                .find(|t| t.id == turn_id)
                .context("active ACP turn missing")?;
            let item = raw_item(turn.items.len(), update)?;
            turn.items.push(item.clone());
            self.emit(
                &id,
                Some(&turn_id),
                "acp.update",
                serde_json::to_value(item)?,
            );
        } else {
            self.emit(
                &id,
                None,
                "acp.session_update",
                serde_json::to_value(update)?,
            );
        }
        Ok(())
    }
}
fn raw_item(index: usize, update: SessionUpdate) -> Result<Item> {
    let text = match &update {
        SessionUpdate::AgentMessageChunk(c) | SessionUpdate::UserMessageChunk(c) => {
            match &c.content {
                ContentBlock::Text(t) => Some(t.text.clone()),
                _ => None,
            }
        }
        _ => None,
    };
    Ok(Item {
        id: index.to_string(),
        kind: "acp.update".into(),
        text,
        provider_data: serde_json::to_value(update)?,
    })
}

pub struct AcpProvider {
    id: String,
    permanent_delete: Option<DeleteCommand>,
    cx: ConnectionTo<Agent>,
    capabilities: AgentCapabilities,
    shared: Arc<Shared>,
    stop: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}
struct ProcessGroup(i32);
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.0),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}
impl AcpProvider {
    pub async fn connect(config: AgentCommand, directory: &Path) -> Result<Self> {
        ensure!(
            !config.id.is_empty()
                && config
                    .id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "agent id must contain letters, digits, '-' or '_'"
        );
        if let Some(delete) = &config.permanent_delete {
            ensure!(
                delete.command.is_some() != delete.method.is_some(),
                "configure exactly one permanentDelete command or method"
            );
            if let Some(command) = &delete.command {
                ensure!(
                    command.is_absolute(),
                    "permanentDelete.command must be absolute"
                );
            }
            if let Some(method) = &delete.method {
                ensure!(
                    method.starts_with('_') && delete.args.is_empty(),
                    "permanentDelete.method must be an explicit ACP extension without command args"
                );
            }
        }
        use std::os::unix::fs::PermissionsExt;
        if !directory.exists() {
            if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty())
                && !parent.exists()
            {
                std::fs::create_dir_all(parent)?;
                std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
            }
            std::fs::create_dir_all(directory)?;
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let path = directory.join(format!("{}.json", config.id));
        let mut records: BTreeMap<String, Record> = if path.exists() {
            serde_json::from_slice(&std::fs::read(&path)?)?
        } else {
            BTreeMap::new()
        };
        for record in records.values_mut() {
            record.session.status = "unloaded".into();
        }
        let (events, _) = broadcast::channel(256);
        let shared = Arc::new(Shared {
            records: Mutex::new(records),
            pending: Mutex::new(vec![]),
            events,
            ready: AtomicBool::new(false),
            path,
        });
        let mut child = Command::new(&config.command)
            .args(&config.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .process_group(0)
            .spawn()
            .with_context(|| {
                format!(
                    "starting ACP agent {} ({})",
                    config.id,
                    config.command.display()
                )
            })?;
        let group = ProcessGroup(child.id().context("missing agent PID")? as i32);
        let input = FramedRead::new(
            child.stdout.take().context("missing stdout")?,
            LinesCodec::new_with_max_length(8 * 1024 * 1024),
        )
        .map_err(std::io::Error::other);
        let output = FramedWrite::new(
            child.stdin.take().context("missing stdin")?,
            LinesCodec::new(),
        )
        .with(|line: String| {
            futures::future::ready(Ok::<_, tokio_util::codec::LinesCodecError>(line))
        })
        .sink_map_err(std::io::Error::other);
        let transport = Lines::new(output, input);
        let stop = CancellationToken::new();
        let done = stop.clone();
        let state = shared.clone();
        let received = shared.clone();
        let (ready, connection) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result = Client.v1()
                .on_receive_notification(async move |notification: SessionNotification, _cx| {
                    received.update(notification).map_err(|e| agent_client_protocol::Error::internal_error().data(e.to_string()))
                }, agent_client_protocol::on_receive_notification!())
                .on_receive_request(async move |_request: UntypedMessage, responder, _cx| {
                    // No client filesystem/terminal capabilities are advertised. Permission
                    // requests receive a standard cancelled outcome until a UI is connected.
                    if _request.method() == "session/request_permission" {
                        responder.respond_with_result(Ok::<_, agent_client_protocol::Error>(json!({"outcome":{"outcome":"cancelled"}})))
                    } else { responder.respond_with_error(agent_client_protocol::Error::method_not_found()) }
                }, agent_client_protocol::on_receive_request!())
                .connect_with(transport, async move |cx| {
                    let _ = ready.send(cx.clone());
                    tokio::select! { _ = done.cancelled() => {}, _ = cx.incoming_closed() => {}, _ = child.wait() => {} }
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    Ok(())
                }).await;
            state.ready.store(false, Ordering::Release);
            if let Err(error) = result {
                tracing::warn!(%error, "ACP agent disconnected");
            }
            let _ = state.events.send(Event {
                session_id: None,
                turn_id: None,
                kind: "provider.unavailable".into(),
                data: json!({}),
            });
            drop(group);
        });
        let cx = connection
            .await
            .context("ACP connection closed during startup")?;
        let initialize = tokio::time::timeout(
            Duration::from_secs(30),
            cx.send_request(
                InitializeRequest::new(agent_client_protocol::schema::ProtocolVersion::V1)
                    .client_info(Implementation::new("tether", env!("CARGO_PKG_VERSION"))),
            )
            .block_task(),
        )
        .await;
        let response = match initialize {
            Ok(Ok(response)) => response,
            other => {
                stop.cancel();
                let _ = task.await;
                bail!("ACP initialization failed: {other:?}");
            }
        };
        if response.protocol_version != agent_client_protocol::schema::ProtocolVersion::V1 {
            stop.cancel();
            let _ = task.await;
            bail!("agent does not support ACP v1");
        }
        shared.ready.store(true, Ordering::Release);
        Ok(Self {
            id: config.id,
            permanent_delete: config.permanent_delete,
            cx,
            capabilities: response.agent_capabilities,
            shared,
            stop,
            task: tokio::sync::Mutex::new(Some(task)),
        })
    }
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        ensure!(self.ready(), "ACP agent is unavailable");
        Ok(tokio::time::timeout(
            Duration::from_secs(30),
            self.cx
                .send_request(UntypedMessage::new(method, params)?)
                .block_task(),
        )
        .await
        .context("ACP request timed out")??)
    }
    fn record(&self, id: &str) -> Result<Record> {
        self.shared
            .records
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .context("unknown ACP session; discover it with session/list first")
    }
    fn check_mcp(&self, mcp: &[McpServer]) -> Result<()> {
        for server in mcp {
            ensure!(
                match server {
                    McpServer::Stdio(_) => true,
                    McpServer::Http(_) => self.capabilities.mcp_capabilities.http,
                    McpServer::Sse(_) => self.capabilities.mcp_capabilities.sse,
                    _ => false,
                },
                "agent does not support this MCP transport"
            );
        }
        Ok(())
    }
    async fn load(&self, id: &str, mcp: &[McpServer]) -> Result<ProviderSession> {
        self.check_mcp(mcp)?;
        let record = self.record(id)?;
        if record.loaded && serde_json::to_value(&record.mcp)? == serde_json::to_value(mcp)? {
            return Ok(record.session);
        }
        ensure!(record.current.is_none(), "cannot reload an active session");
        ensure!(
            self.capabilities.load_session,
            "agent does not support session/load"
        );
        {
            let mut records = self.shared.records.lock().unwrap();
            let entry = records.get_mut(id).unwrap();
            entry.loading = true;
            entry.replay.clear();
        }
        let result = self
            .request(
                "session/load",
                json!({"sessionId":id,"cwd":record.session.cwd,"mcpServers":mcp}),
            )
            .await;
        {
            let mut records = self.shared.records.lock().unwrap();
            let entry = records.get_mut(id).unwrap();
            entry.loading = false;
            if let Ok(response) = &result {
                let response: LoadSessionResponse = serde_json::from_value(response.clone())?;
                entry.config = response.config_options.unwrap_or_default();
                entry.modes = response.modes;
                entry.loaded = true;
                entry.mcp = mcp.to_vec();
                entry.session.status = "idle".into();
                // For external/direct sessions ACP supplies the authoritative replay.
                // Managed sessions already have durable Tether history: never import it twice.
                if entry.direct {
                    entry.turns = vec![Turn {
                        id: format!("replay:{id}"),
                        status: "completed".into(),
                        items: std::mem::take(&mut entry.replay),
                        error: None,
                    }];
                }
                entry.replay.clear();
            }
        }
        result?;
        // Restore only explicitly selected values. Defaults and dependent controls
        // are taken from the adapter's updated catalog after every response.
        let mut selected: Vec<_> = record.selected.into_iter().collect();
        selected.sort_by_key(|(id, _)| {
            record
                .config
                .iter()
                .position(|c| c.id.to_string() == *id)
                .unwrap_or(usize::MAX)
        });
        for (key, value) in selected {
            if let Err(error) = self.set_config(id, &key, value).await {
                self.shared
                    .records
                    .lock()
                    .unwrap()
                    .get_mut(id)
                    .unwrap()
                    .loaded = false;
                return Err(error.context("restoring ACP configuration"));
            }
        }
        self.shared.save()?;
        Ok(self.record(id)?.session)
    }
}
#[async_trait]
impl Provider for AcpProvider {
    fn supports_delete(&self) -> bool {
        self.permanent_delete.is_some() && self.supports_close()
    }
    async fn delete(&self, id: &str) -> Result<()> {
        let delete = self
            .permanent_delete
            .as_ref()
            .context("no permanent deletion command configured")?;
        ensure!(self.ready(), "agent is unavailable");
        let record = self.record(id)?;
        ensure!(
            record.current.is_none(),
            "cannot delete an active native conversation"
        );
        if record.loaded {
            self.close(id).await?;
        }
        // Standard ACP deletion can mean archive. Only explicitly configured
        // permanent deletion operations are eligible here.
        if let Some(method) = &delete.method {
            self.request(method, json!({"sessionId": id})).await?;
        } else {
            let mut child = Command::new(
                delete
                    .command
                    .as_ref()
                    .context("missing deletion command")?,
            )
            .args(&delete.args)
            .arg("--")
            .arg(id)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0)
            .spawn()
            .context("starting permanent deletion command")?;
            let _group = ProcessGroup(child.id().context("missing deletion command PID")? as i32);
            let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
                .await
                .context("permanent deletion command timed out; outcome is unknown")??;
            ensure!(
                status.success(),
                "permanent deletion command failed ({status})"
            );
        }
        // Verify against the provider's fresh catalog, not the local record cache.
        // Codex's supplied adapter includes archived histories in this catalog.
        let mut cursor = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = self
                .list(cursor)
                .await
                .context("verifying permanent deletion")?;
            ensure!(
                !page.data.iter().any(|session| session.id == id),
                "native conversation still exists after deletion"
            );
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
            ensure!(
                seen.insert(cursor.clone()),
                "native deletion verification returned a repeated cursor"
            );
        }
        self.shared.records.lock().unwrap().remove(id);
        self.shared
            .pending
            .lock()
            .unwrap()
            .retain(|update| update.session_id.to_string() != id);
        self.shared.save()?;
        Ok(())
    }
    fn supports_close(&self) -> bool {
        self.capabilities.session_capabilities.close.is_some() && self.capabilities.load_session
    }
    async fn close(&self, id: &str) -> Result<Vec<Turn>> {
        let record = self.record(id)?;
        if !record.loaded {
            return Ok(vec![]);
        }
        self.request("session/close", json!({"sessionId":id}))
            .await?;
        tokio::time::timeout(Duration::from_secs(30), async {
            while self.record(id)?.current.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("waiting for closed session's prompt to settle")??;
        let turns = {
            let mut records = self.shared.records.lock().unwrap();
            let entry = records.get_mut(id).context("unknown ACP session")?;
            entry.loaded = false;
            entry.session.status = "unloaded".into();
            entry.updates.clear();
            entry.replay.clear();
            std::mem::take(&mut entry.turns)
        };
        self.shared.save()?;
        Ok(turns)
    }
    async fn modes(&self, id: &str) -> Result<Option<SessionModeState>> {
        Ok(self.record(id)?.modes)
    }
    async fn set_mode(&self, id: &str, mode: SessionModeId) -> Result<()> {
        let record = self.record(id)?;
        let modes = record
            .modes
            .ok_or_else(agent_client_protocol::Error::method_not_found)?;
        if !modes.available_modes.iter().any(|m| m.id == mode) {
            return Err(agent_client_protocol::Error::invalid_params()
                .data("unknown session mode")
                .into());
        }
        self.request(
            "session/set_mode",
            serde_json::to_value(SetSessionModeRequest::new(id.to_owned(), mode.clone()))?,
        )
        .await?;
        {
            let mut records = self.shared.records.lock().unwrap();
            let entry = records.get_mut(id).context("unknown ACP session")?;
            if let Some(modes) = &mut entry.modes {
                modes.current_mode_id = mode.clone();
            }
        }
        self.shared.emit(
            id,
            None,
            "acp.session_update",
            serde_json::to_value(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                mode,
            )))?,
        );
        self.shared.save()?;
        Ok(())
    }
    fn id(&self) -> &str {
        &self.id
    }
    async fn session_updates(&self, id: &str) -> Result<Vec<SessionUpdate>> {
        Ok(self.record(id)?.updates.into_values().collect())
    }
    fn mcp_capabilities(&self) -> McpCapabilities {
        self.capabilities.mcp_capabilities.clone()
    }
    async fn managed(&self, id: &str) -> Result<()> {
        let changed = if let Some(record) = self.shared.records.lock().unwrap().get_mut(id) {
            std::mem::replace(&mut record.direct, false)
        } else {
            false
        };
        if changed {
            self.shared.save()?;
        }
        Ok(())
    }
    fn prompt_capabilities(&self) -> PromptCapabilities {
        self.capabilities.prompt_capabilities.clone()
    }
    fn ready(&self) -> bool {
        self.shared.ready.load(Ordering::Acquire) && !self.stop.is_cancelled()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            delete: self.supports_delete(),
            resume: self.capabilities.load_session,
            list: self.capabilities.session_capabilities.list.is_some(),
            history: self.capabilities.load_session,
            interrupt: true,
            switch: self.capabilities.load_session,
        }
    }
    fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.shared.events.subscribe()
    }
    async fn create(&self, request: CreateSession) -> Result<ProviderSession> {
        self.check_mcp(&request.mcp_servers)?;
        let result: NewSessionResponse = serde_json::from_value(
            self.request(
                "session/new",
                json!({"cwd":request.cwd,"mcpServers":request.mcp_servers}),
            )
            .await?,
        )?;
        let session = ProviderSession {
            id: result.session_id.to_string(),
            provider: self.id.clone(),
            cwd: request.cwd,
            status: "idle".into(),
            title: None,
        };
        self.shared.records.lock().unwrap().insert(
            session.id.clone(),
            Record {
                session: session.clone(),
                config: result.config_options.unwrap_or_default(),
                modes: result.modes,
                selected: BTreeMap::new(),
                mcp: request.mcp_servers,
                direct: request.kind == SessionKind::Direct,
                loaded: true,
                loading: false,
                current: None,
                turns: vec![],
                replay: vec![],
                updates: BTreeMap::new(),
            },
        );
        let pending = std::mem::take(&mut *self.shared.pending.lock().unwrap());
        for update in pending {
            if update.session_id == result.session_id {
                self.shared.update(update)?;
            }
        }
        self.shared.save()?;
        Ok(session)
    }
    async fn list(&self, cursor: Option<String>) -> Result<Page<ProviderSession>> {
        ensure!(
            self.capabilities.session_capabilities.list.is_some(),
            "agent does not advertise session/list"
        );
        let result: ListSessionsResponse = serde_json::from_value(
            self.request("session/list", json!({"cursor":cursor}))
                .await?,
        )?;
        let mut data = Vec::new();
        {
            let mut records = self.shared.records.lock().unwrap();
            for info in result.sessions {
                let session = ProviderSession {
                    id: info.session_id.to_string(),
                    provider: self.id.clone(),
                    cwd: info.cwd.to_string_lossy().into(),
                    title: info.title,
                    status: "unloaded".into(),
                };
                let entry = records.entry(session.id.clone()).or_insert_with(|| Record {
                    session: session.clone(),
                    config: vec![],
                    modes: None,
                    selected: BTreeMap::new(),
                    mcp: vec![],
                    direct: true,
                    loaded: false,
                    loading: false,
                    current: None,
                    turns: vec![],
                    replay: vec![],
                    updates: BTreeMap::new(),
                });
                // Cwd is authoritative from the agent, even for known IDs.
                entry.session.cwd = session.cwd;
                entry.session.title = session.title;
                data.push(entry.session.clone());
            }
        }
        self.shared.save()?;
        Ok(Page {
            data,
            next_cursor: result.next_cursor,
        })
    }
    async fn get(&self, id: &str) -> Result<ProviderSession> {
        ensure!(self.ready(), "ACP agent is unavailable");
        if let Ok(record) = self.record(id) {
            return Ok(record.session);
        }
        let mut cursor = None;
        loop {
            let page = self.list(cursor).await?;
            if let Some(session) = page.data.into_iter().find(|s| s.id == id) {
                return Ok(session);
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                bail!("ACP session not found")
            }
        }
    }
    async fn history(&self, id: &str) -> Result<Vec<Turn>> {
        let record = self.record(id)?;
        if record.direct && !record.loaded {
            self.load(id, &record.mcp).await?;
        }
        Ok(self.record(id)?.turns)
    }
    async fn resume(&self, id: &str, mcp: &[McpServer]) -> Result<ProviderSession> {
        self.load(id, mcp).await
    }
    async fn release(&self, _id: &str) -> Result<()> {
        // Switching the active binding does not require destroying its native session.
        // Keeping it loaded avoids a second replay and preserves native caches.
        Ok(())
    }
    async fn config(&self, id: &str) -> Result<Vec<SessionConfigOption>> {
        Ok(self.record(id)?.config)
    }
    async fn set_config(
        &self,
        id: &str,
        key: &str,
        value: SessionConfigOptionValue,
    ) -> Result<Vec<SessionConfigOption>> {
        let request =
            SetSessionConfigOptionRequest::new(id.to_owned(), key.to_owned(), value.clone());
        let result: SetSessionConfigOptionResponse = serde_json::from_value(
            self.request("session/set_config_option", serde_json::to_value(request)?)
                .await?,
        )?;
        {
            let mut records = self.shared.records.lock().unwrap();
            let record = records.get_mut(id).context("unknown ACP session")?;
            record.config = result.config_options.clone();
            // Dependent controls may disappear or change after model selection. Retain
            // only values the returned catalog still confirms.
            record.selected.insert(key.into(), value);
            record.selected.retain(|id, value| {
                record.config.iter().any(|c| {
                    c.id.to_string() == *id
                        && serde_json::to_value(&c.kind)
                            .ok()
                            .and_then(|v| v.get("currentValue").cloned())
                            == serde_json::to_value(&*value)
                                .ok()
                                .and_then(|v| v.get("value").cloned())
                })
            });
        }
        self.shared.save()?;
        Ok(result.config_options)
    }
    async fn prompt(&self, id: &str, content: Vec<ContentBlock>) -> Result<StartedTurn> {
        ensure!(self.ready(), "ACP agent is unavailable");
        for block in &content {
            ensure!(
                match block {
                    ContentBlock::Text(_) | ContentBlock::ResourceLink(_) => true,
                    ContentBlock::Image(_) => self.capabilities.prompt_capabilities.image,
                    ContentBlock::Audio(_) => self.capabilities.prompt_capabilities.audio,
                    ContentBlock::Resource(_) =>
                        self.capabilities.prompt_capabilities.embedded_context,
                    _ => false,
                },
                "agent does not support this prompt content type"
            );
        }
        let user_items = content
            .iter()
            .enumerate()
            .map(|(index, block)| {
                Ok(Item {
                    id: format!("user:{index}"),
                    kind: "message.user".into(),
                    text: match block {
                        ContentBlock::Text(t) => Some(t.text.clone()),
                        _ => None,
                    },
                    provider_data: serde_json::to_value(block)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let turn_id = uuid::Uuid::new_v4().to_string();
        {
            let mut records = self.shared.records.lock().unwrap();
            let record = records.get_mut(id).context("unknown ACP session")?;
            ensure!(
                record.current.is_none() && record.loaded,
                "ACP session is not idle and loaded"
            );
            record.current = Some(turn_id.clone());
            record.session.status = "running".into();
            record.turns.push(Turn {
                id: turn_id.clone(),
                status: "inProgress".into(),
                items: user_items,
                error: None,
            });
        }
        let state = self.shared.clone();
        // Publish before returning StartedTurn so an immediate cancel follows it.
        let response = self
            .cx
            .send_request(PromptRequest::new(id.to_owned(), content))
            .block_task();
        let id = id.to_owned();
        let native_turn = turn_id.clone();
        tokio::spawn(async move {
            // ACP has no receipt acknowledgement. Retire handover only on a
            // successful prompt response; failures remain ambiguous and are not retried.
            let result = response.await;
            let (kind, status, data) = match result {
                Ok(response) => {
                    let cancelled = response.stop_reason == StopReason::Cancelled;
                    if !cancelled {
                        state.emit(&id, Some(&native_turn), "message.accepted", json!({}));
                    }
                    (
                        if cancelled {
                            "turn.interrupted"
                        } else {
                            "turn.completed"
                        },
                        if cancelled {
                            "interrupted"
                        } else {
                            "completed"
                        },
                        serde_json::to_value(response).unwrap(),
                    )
                }
                Err(error) => ("turn.failed", "failed", json!({"error":error})),
            };
            {
                let mut records = state.records.lock().unwrap();
                if let Some(record) = records.get_mut(&id) {
                    record.current = None;
                    record.session.status = "idle".into();
                    if let Some(turn) = record.turns.iter_mut().find(|t| t.id == native_turn) {
                        turn.status = status.into();
                        turn.error = data.get("error").cloned();
                    }
                }
            }
            state.emit(&id, Some(&native_turn), kind, data);
        });
        Ok(StartedTurn {
            id: turn_id,
            accepted: false,
        })
    }
    async fn interrupt(&self, id: &str, _turn: &str) -> Result<()> {
        self.cx
            .send_notification(CancelNotification::new(id.to_owned()))?;
        Ok(())
    }
    async fn shutdown(&self) {
        self.stop.cancel();
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }
}

impl Drop for AcpProvider {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
