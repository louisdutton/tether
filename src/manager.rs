mod delivery;
use crate::{model::*, providers::Provider, store::Store};
use agent_client_protocol::v1::schema::{
    ContentBlock, McpCapabilities, PromptCapabilities, SessionConfigOption,
    SessionConfigOptionValue, SessionModeId, SessionModeState, SessionUpdate,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StoreMutex},
};
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    NotFound,
    Busy,
    Deletion(String),
    Provider(anyhow::Error),
    Storage(anyhow::Error),
}
impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Self::Provider(e)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

pub struct Manager {
    providers: BTreeMap<String, Arc<dyn Provider>>,
    root: PathBuf,
    store: StoreMutex<Option<Store>>,
    // Provider notifications wait until command results and authored inputs are saved.
    commands: Mutex<()>,
    epochs: StoreMutex<BTreeMap<String, u64>>,
    events: broadcast::Sender<Event>,
    stop: CancellationToken,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Manager {
    pub async fn open(
        root: PathBuf,
        database: &Path,
        providers: Vec<Arc<dyn Provider>>,
    ) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(!providers.is_empty(), "at least one provider is required");
        let mut registry = BTreeMap::new();
        for provider in providers {
            anyhow::ensure!(
                registry
                    .insert(provider.id().to_owned(), provider)
                    .is_none(),
                "duplicate provider"
            );
        }
        let (events, _) = broadcast::channel(256);
        let manager = Arc::new(Self {
            providers: registry,
            root: root.canonicalize()?,
            store: StoreMutex::new(Some(Store::open(database)?)),
            commands: Mutex::new(()),
            epochs: StoreMutex::new(BTreeMap::new()),
            events,
            stop: CancellationToken::new(),
            tasks: Mutex::new(Vec::new()),
        });
        for mut session in manager
            .db(|db| db.sessions())
            .map_err(|e| anyhow::anyhow!("reading stored sessions: {e:?}"))?
        {
            if session.status == "running" {
                session.status = "unloaded".into();
                manager
                    .db(|db| db.save(&session))
                    .map_err(|e| anyhow::anyhow!("restoring session: {e:?}"))?;
            }
            if session.kind == SessionKind::Managed {
                for binding in session.bindings {
                    if let Some(provider) = manager.providers.get(&binding.provider) {
                        provider.managed(&binding.session_id).await?;
                    }
                }
            }
        }
        for (id, provider) in &manager.providers {
            let mut receiver = provider.subscribe();
            let weak = Arc::downgrade(&manager);
            let stop = manager.stop.clone();
            let id = id.clone();
            let task = tokio::spawn(async move {
                loop {
                    let event = tokio::select! {
                        _ = stop.cancelled() => match receiver.try_recv() {
                            Ok(event) => event,
                            Err(_) => break,
                        },
                        result = receiver.recv() => match result {
                            Ok(event) => event,
                            Err(broadcast::error::RecvError::Lagged(_)) => Event {
                                session_id: None, turn_id: None, kind: "resync.required".into(), data: serde_json::json!({}),
                            },
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    };
                    let Some(manager) = weak.upgrade() else { break };
                    let _guard = manager.commands.lock().await;
                    if let Err(error) = manager.receive(&id, event).await {
                        tracing::error!(?error, "session event could not be persisted");
                        let _ = manager.events.send(Event {
                            session_id: None,
                            turn_id: None,
                            kind: "resync.required".into(),
                            data: serde_json::json!({"provider":id}),
                        });
                    }
                }
            });
            manager.tasks.lock().await.push(task);
        }
        manager.start_queue_worker().await;
        Ok(manager)
    }

    fn db<T>(&self, f: impl FnOnce(&Store) -> anyhow::Result<T>) -> Result<T> {
        let store = self.store.lock().expect("store mutex poisoned");
        let store = store
            .as_ref()
            .ok_or_else(|| Error::Storage(anyhow::anyhow!("session store is closed")))?;
        f(store).map_err(Error::Storage)
    }
    fn provider(&self, id: &str) -> Result<&Arc<dyn Provider>> {
        self.providers
            .get(id)
            .ok_or_else(|| Error::Invalid(format!("provider is not configured: {id}")))
    }
    pub fn ready(&self) -> bool {
        self.providers.values().any(|p| p.ready())
    }
    pub fn providers(&self) -> Vec<ProviderInfo> {
        self.providers
            .iter()
            .map(|(id, p)| ProviderInfo {
                id: id.clone(),
                ready: p.ready(),
                capabilities: p.capabilities(),
            })
            .collect()
    }
    pub fn default_provider(&self) -> &str {
        if self.providers.get("codex").is_some_and(|p| p.ready()) {
            "codex"
        } else {
            self.providers
                .iter()
                .find(|(_, p)| p.ready())
                .map(|(id, _)| id.as_str())
                .unwrap_or_else(|| self.providers.keys().next().expect("at least one provider"))
        }
    }
    pub fn mcp_capabilities(&self) -> McpCapabilities {
        McpCapabilities::new()
            .http(self.providers.values().all(|p| p.mcp_capabilities().http))
            .sse(self.providers.values().all(|p| p.mcp_capabilities().sse))
    }
    pub fn prompt_capabilities(&self) -> PromptCapabilities {
        let all = |f: fn(&PromptCapabilities) -> bool| {
            self.providers.values().all(|p| f(&p.prompt_capabilities()))
        };
        PromptCapabilities::new()
            .image(all(|c| c.image))
            .audio(all(|c| c.audio))
            .embedded_context(all(|c| c.embedded_context))
    }
    pub(crate) fn session_epoch(&self, id: &str) -> u64 {
        self.epochs.lock().unwrap().get(id).copied().unwrap_or(0)
    }
    fn detached(&self, id: &str, kind: &str) {
        let epoch = {
            let mut epochs = self.epochs.lock().unwrap();
            let epoch = epochs.entry(id.into()).or_default();
            *epoch += 1;
            *epoch
        };
        let _ = self.events.send(Event {
            session_id: Some(id.into()),
            turn_id: None,
            kind: kind.into(),
            data: serde_json::json!({"epoch":epoch}),
        });
    }
    pub fn supports_close(&self) -> bool {
        self.providers.values().all(|p| p.supports_close())
    }
    pub async fn modes(&self, id: &str) -> Result<Option<SessionModeState>> {
        let session = self.stored(id)?;
        match self.providers.get(&session.provider) {
            Some(provider) => Ok(provider.modes(&session.binding().session_id).await?),
            None => Ok(None),
        }
    }
    pub async fn set_mode(&self, id: &str, mode: SessionModeId) -> Result<()> {
        let _guard = self.commands.lock().await;
        let session = self.executable(id)?;
        let provider = self.provider(&session.provider)?;
        if self.native(&session).await?.status != "running" {
            provider
                .resume(&session.binding().session_id, &session.mcp_servers)
                .await?;
        }
        provider
            .set_mode(&session.binding().session_id, mode)
            .await?;
        Ok(())
    }
    pub async fn delete(&self, id: &str) -> Result<()> {
        let _guard = self.commands.lock().await;
        let Some(mut session) = self.db(|db| db.get(id))? else {
            return Ok(());
        };
        for binding in session.bindings.iter().filter(|b| !b.deleted) {
            let problem = match self.providers.get(&binding.provider) {
                None => Some("agent is not configured".to_string()),
                Some(provider) if !provider.supports_delete() => {
                    Some("permanent deletion is unsupported".into())
                }
                Some(provider) if !provider.ready() => Some("agent is unavailable".into()),
                Some(provider) => match provider.get(&binding.session_id).await {
                    Ok(native) if native.status == "running" => return Err(Error::Busy),
                    Ok(_) => None,
                    Err(error) => Some(format!("cannot verify native conversation: {error}")),
                },
            };
            if let Some(problem) = problem {
                let message = format!("{}: {problem}", binding.provider);
                session.deletion_error = Some(message.clone());
                self.db(|db| db.save(&session))?;
                return Err(Error::Deletion(message));
            }
        }
        session.deletion_pending = true;
        session.deletion_error = None;
        self.db(|db| db.save(&session))?;
        for index in 0..session.bindings.len() {
            let binding = &session.bindings[index];
            if binding.deleted {
                continue;
            }
            if let Err(error) = self
                .provider(&binding.provider)?
                .delete(&binding.session_id)
                .await
            {
                let message = format!("{}: {error}", binding.provider);
                session.deletion_error = Some(message.clone());
                self.db(|db| db.save(&session))?;
                return Err(Error::Deletion(message));
            }
            session.bindings[index].deleted = true;
            self.db(|db| db.save(&session))?;
        }
        self.db(|db| db.delete(id))?;
        self.detached(id, "session.deleted");
        Ok(())
    }
    pub async fn close(&self, id: &str) -> Result<()> {
        let _guard = self.commands.lock().await;
        let Some(mut session) = self.db(|db| db.get(id))? else {
            return Ok(());
        };
        if !self.allowed(&session.cwd) {
            return Ok(());
        }
        self.db(|db| db.pause_queue(id))?;
        self.queue_changed(id)?;
        for binding in &session.bindings {
            if let Some(provider) = self.providers.get(&binding.provider) {
                let turns = provider.close(&binding.session_id).await?;
                if session.kind == SessionKind::Managed {
                    self.db(|db| {
                        db.transaction(|db| {
                            for turn in &turns {
                                db.put_turn(id, &binding.provider, &binding.session_id, turn)?;
                            }
                            Ok(())
                        })
                    })?;
                }
            }
        }
        session.status = "unloaded".into();
        self.db(|db| db.save(&session))?;
        self.detached(id, "session.closed");
        Ok(())
    }
    pub async fn session_updates(&self, id: &str) -> Result<Vec<SessionUpdate>> {
        let session = self.stored(id)?;
        match self.providers.get(&session.provider) {
            Some(provider) => Ok(provider
                .session_updates(&session.binding().session_id)
                .await
                .unwrap_or_default()),
            None => Ok(vec![]),
        }
    }
    pub async fn config(&self, id: &str) -> Result<Vec<SessionConfigOption>> {
        let session = self.stored(id)?;
        let mut agents: Vec<_> = self
            .providers
            .iter()
            .filter(|(id, p)| {
                **id == session.provider
                    || (session.kind == SessionKind::Managed
                        && p.ready()
                        && p.capabilities().switch)
            })
            .map(|(id, _)| serde_json::json!({"value":id,"name":id}))
            .collect();
        if !self.providers.contains_key(&session.provider) {
            agents.insert(
                0,
                serde_json::json!({"value":session.provider,"name":session.provider}),
            );
        }
        let mut options = vec![serde_json::from_value(serde_json::json!({
            "id":"tether.agent","name":"Agent","type":"select","currentValue":session.provider,"options":agents
        })).map_err(anyhow::Error::from)?];
        if let Some(provider) = self.providers.get(&session.provider) {
            for mut option in provider
                .config(&session.binding().session_id)
                .await
                .unwrap_or_default()
            {
                // Keep provider option IDs opaque and avoid collisions with Tether's selector.
                option.id = format!("agent/{}", option.id).into();
                options.push(option);
            }
        }
        Ok(options)
    }
    pub async fn set_config(
        &self,
        id: &str,
        key: &str,
        value: SessionConfigOptionValue,
    ) -> Result<Vec<SessionConfigOption>> {
        let _guard = self.commands.lock().await;
        let mut session = self.executable(id)?;
        if key == "tether.agent" {
            let target = value
                .as_value_id()
                .ok_or_else(|| Error::Invalid("agent must be a select value".into()))?
                .to_string();
            if target != session.provider {
                self.switch(&mut session, &target).await?;
            }
        } else {
            let key = key
                .strip_prefix("agent/")
                .ok_or_else(|| Error::Invalid("unknown configuration option".into()))?;
            let provider = self.provider(&session.provider)?;
            if self.native(&session).await?.status != "running" {
                provider
                    .resume(&session.binding().session_id, &session.mcp_servers)
                    .await?;
            }
            provider
                .set_config(&session.binding().session_id, key, value)
                .await?;
        }
        let options = self.config(id).await?;
        let _ = self.events.send(Event { session_id: Some(id.into()), turn_id: None, kind: "acp.session_update".into(), data: serde_json::json!({"sessionUpdate":"config_option_update","configOptions":options}) });
        Ok(options)
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }
    fn allowed(&self, cwd: &str) -> bool {
        Path::new(cwd).is_absolute()
            && Path::new(cwd)
                .canonicalize()
                .is_ok_and(|p| p.is_dir() && p.starts_with(&self.root))
    }
    fn stored(&self, id: &str) -> Result<Session> {
        let session = self.db(|db| db.get(id))?.ok_or(Error::NotFound)?;
        if !self.allowed(&session.cwd) {
            return Err(Error::NotFound);
        }
        Ok(session)
    }
    fn executable(&self, id: &str) -> Result<Session> {
        let session = self.stored(id)?;
        if session.deletion_pending {
            return Err(Error::Deletion(
                "permanent deletion is incomplete; retry delete before continuing".into(),
            ));
        }
        Ok(session)
    }
    async fn native(&self, session: &Session) -> Result<ProviderSession> {
        let binding = session.binding();
        let native = self
            .provider(&binding.provider)?
            .get(&binding.session_id)
            .await?;
        if session.kind == SessionKind::Managed {
            self.provider(&binding.provider)?
                .managed(&binding.session_id)
                .await?;
        }
        if native.id != binding.session_id
            || native.provider != binding.provider
            || !self.allowed(&native.cwd)
            || Path::new(&native.cwd).canonicalize().ok()
                != Path::new(&session.cwd).canonicalize().ok()
        {
            return Err(Error::NotFound);
        }
        Ok(native)
    }
    pub fn agent_changes(&self, id: &str) -> Result<Vec<AgentChange>> {
        Ok(self.stored(id)?.agent_changes)
    }

    fn record(
        &self,
        native: ProviderSession,
        kind: SessionKind,
        turns: &[Turn],
        mcp_servers: Vec<agent_client_protocol::v1::schema::McpServer>,
    ) -> Result<Session> {
        if self
            .db(|db| db.owner(&native.provider, &native.id))?
            .is_some()
        {
            return Err(Error::Invalid(
                "native conversation already belongs to a Tether session".into(),
            ));
        }
        let session = Session {
            updated_at: native.updated_at,
            internal: native.internal,
            deletion_pending: false,
            deletion_error: None,
            mcp_servers,
            id: Uuid::new_v4().to_string(),
            kind,
            machine_id: "local".into(),
            provider: native.provider.clone(),
            cwd: native.cwd,
            status: native.status,
            title: native.title,
            bindings: vec![Binding {
                provider: native.provider,
                session_id: native.id,
                replay_through: 0,
                deleted: false,
            }],
            agent_changes: vec![],
            pending_context: None,
            pending_turn: None,
        };
        self.db(|db| db.save_with_history(&session, turns))?;
        Ok(session)
    }
    pub async fn create(&self, mut request: CreateSession) -> Result<Session> {
        let _guard = self.commands.lock().await;
        let provider = self.provider(&request.provider)?;
        if !self.allowed(&request.cwd) {
            return Err(Error::Invalid(
                "cwd must be an existing directory inside the workspace root".into(),
            ));
        }
        request.cwd = Path::new(&request.cwd)
            .canonicalize()
            .map_err(anyhow::Error::from)?
            .to_string_lossy()
            .into();
        let kind = request.kind;
        crate::providers::validate_mcp(&request.mcp_servers)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let mcp_servers = request.mcp_servers.clone();
        let session = self.record(provider.create(request).await?, kind, &[], mcp_servers)?;
        if kind == SessionKind::Managed {
            provider.managed(&session.binding().session_id).await?;
        }
        Ok(session)
    }
    pub async fn adopt(&self, request: AdoptSession) -> Result<Session> {
        let _guard = self.commands.lock().await;
        let provider = self.provider(&request.provider)?;
        let native = provider.get(&request.provider_session_id).await?;
        if !self.allowed(&native.cwd) {
            return Err(Error::NotFound);
        }
        if native.status == "running" {
            return Err(Error::Busy);
        }
        // Read before creating a managed identity, so a failed import changes nothing.
        let turns = if request.kind == SessionKind::Managed {
            Some(provider.history(&native.id).await?)
        } else {
            None
        };
        let session =
            if let Some(mut existing) = self.db(|db| db.owner(&native.provider, &native.id))? {
                if existing.deletion_pending {
                    return Err(Error::Deletion(
                        "conversation deletion is incomplete".into(),
                    ));
                }
                if request.kind == SessionKind::Managed && existing.kind == SessionKind::Direct {
                    existing.kind = SessionKind::Managed;
                    self.db(|db| {
                        db.save_with_history(&existing, turns.as_deref().unwrap_or_default())
                    })?;
                }
                existing
            } else {
                self.record(native, request.kind, &turns.unwrap_or_default(), vec![])?
            };
        if request.kind == SessionKind::Managed {
            provider.managed(&session.binding().session_id).await?;
        }
        Ok(session)
    }
    pub async fn import(&self, id: &str) -> Result<Session> {
        let _guard = self.commands.lock().await;
        let mut session = self.executable(id)?;
        if session.kind == SessionKind::Managed {
            return Ok(session);
        }
        let provider = self.provider(&session.provider)?;
        let native = self.native(&session).await?;
        if native.status == "running" {
            return Err(Error::Busy);
        }
        let turns = provider.history(&native.id).await?;
        session.kind = SessionKind::Managed;
        self.db(|db| db.save_with_history(&session, &turns))?;
        provider.managed(&session.binding().session_id).await?;
        Ok(session)
    }
    pub async fn discover(
        &self,
        provider: &str,
        cursor: Option<String>,
    ) -> Result<Page<ProviderSession>> {
        let mut page = self.provider(provider)?.list(cursor).await?;
        page.data.retain(|s| self.allowed(&s.cwd));
        Ok(page)
    }
    pub async fn list(&self, cursor: Option<String>) -> Result<CatalogPage> {
        self.list_filtered(cursor, None).await
    }
    pub async fn list_filtered(
        &self,
        cursor: Option<String>,
        cwd: Option<&Path>,
    ) -> Result<CatalogPage> {
        self.catalog_list(cursor, cwd, false).await
    }
    pub async fn discover_and_list(
        &self,
        cursor: Option<String>,
        cwd: Option<&Path>,
    ) -> Result<CatalogPage> {
        if cursor.is_some() {
            return Err(Error::Invalid(
                "discovery requires a fresh session list".into(),
            ));
        }
        self.catalog_list(cursor, cwd, true).await
    }
    async fn catalog_list(
        &self,
        cursor: Option<String>,
        cwd: Option<&Path>,
        discover: bool,
    ) -> Result<CatalogPage> {
        // Known catalog reads must not wait for a provider discovery or active command.
        let _guard = if discover {
            Some(self.commands.lock().await)
        } else {
            None
        };
        let cwd = cwd
            .map(|path| {
                path.canonicalize()
                    .map(|p| p.to_string_lossy().into_owned())
            })
            .transpose()
            .map_err(|_| Error::Invalid("cwd is unavailable".into()))?;
        if let Some(cursor) = cursor {
            let parts: Vec<_> = cursor.split(':').collect();
            if parts.len() != 3 || parts[0] != "v2" || Uuid::parse_str(parts[1]).is_err() {
                return Err(Error::Invalid(
                    "invalid catalog cursor; refresh the session list".into(),
                ));
            }
            let offset: usize = parts[2]
                .parse()
                .map_err(|_| Error::Invalid("invalid catalog cursor".into()))?;
            let snapshot = self.db(|db| db.catalog(parts[1]))?.ok_or_else(|| {
                Error::Invalid("catalog cursor expired; refresh the session list".into())
            })?;
            if snapshot.cwd != cwd || offset >= snapshot.ids.len() {
                return Err(Error::Invalid(
                    "catalog cursor does not match this session list".into(),
                ));
            }
            return self.catalog_page(parts[1], &snapshot, offset).await;
        }
        let mut failures = vec![];
        if discover {
            for (id, provider) in &self.providers {
                if !provider.ready() || !provider.capabilities().list {
                    failures.push(CatalogFailure {
                    provider: id.clone(),
                    message:
                        "native catalog is unavailable or unsupported; showing cached conversations"
                            .into(),
                });
                    continue;
                }
                let mut cursor = None;
                let mut seen = std::collections::HashSet::new();
                let mut discovered = vec![];
                loop {
                    match provider.list(cursor.clone()).await {
                        Ok(page) => {
                            discovered.extend(page.data);
                            match page.next_cursor {
                                None => break,
                                Some(next) if seen.insert(next.clone()) => cursor = Some(next),
                                Some(_) => {
                                    failures.push(CatalogFailure {
                                        provider: id.clone(),
                                        message: "agent returned a repeated catalog cursor".into(),
                                    });
                                    break;
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                provider = id,
                                ?error,
                                "native session catalog refresh failed"
                            );
                            failures.push(CatalogFailure {
                                provider: id.clone(),
                                message:
                                    "native catalog refresh failed; showing cached conversations"
                                        .into(),
                            });
                            break;
                        }
                    }
                }
                for native in discovered {
                    if native.provider != *id || !self.allowed(&native.cwd) {
                        continue;
                    }
                    if let Some(mut session) = self.db(|db| db.owner(id, &native.id))? {
                        session.updated_at =
                            latest_activity(session.updated_at, native.updated_at.clone());
                        if session.provider == *id && !session.deletion_pending {
                            session.title = native.title.clone().or(session.title);
                            session.internal = native.internal;
                        }
                        self.db(|db| db.save(&session))?;
                        continue;
                    }
                    if !native.internal {
                        self.record(native, SessionKind::Direct, &[], vec![])?;
                    }
                }
            }
        }
        let mut sessions: Vec<_> = self
            .db(|db| db.sessions())?
            .into_iter()
            .filter(|s| !s.internal)
            .filter(|s| self.allowed(&s.cwd) || s.deletion_pending || s.deletion_error.is_some())
            .filter(|s| {
                cwd.as_ref().is_none_or(|cwd| {
                    Path::new(&s.cwd)
                        .canonicalize()
                        .ok()
                        .is_some_and(|path| path.to_string_lossy().as_ref() == cwd)
                })
            })
            .collect();
        sessions.sort_by(|a, b| {
            b.updated_at
                .as_deref()
                .and_then(activity_timestamp)
                .cmp(&a.updated_at.as_deref().and_then(activity_timestamp))
                .then_with(|| a.id.cmp(&b.id))
        });
        let snapshot = CatalogSnapshot {
            ids: sessions.into_iter().map(|s| s.id).collect(),
            cwd,
            failures,
        };
        let id = Uuid::new_v4().to_string();
        if snapshot.ids.len() > 50 {
            self.db(|db| db.save_catalog(&id, &snapshot))?;
        }
        self.catalog_page(&id, &snapshot, 0).await
    }
    async fn catalog_page(
        &self,
        id: &str,
        snapshot: &CatalogSnapshot,
        mut offset: usize,
    ) -> Result<CatalogPage> {
        let mut data = vec![];
        while offset < snapshot.ids.len() && data.len() < 50 {
            let session_id = &snapshot.ids[offset];
            offset += 1;
            // A deletion or internal classification after the snapshot must not resurrect a row.
            let Some(mut session) = self.db(|db| db.get(session_id))? else {
                continue;
            };
            if session.internal {
                continue;
            }
            if session.deletion_pending {
                session.status = "deletion_failed".into();
            } else if !self.allowed(&session.cwd) {
                if session.deletion_error.is_none() {
                    continue;
                }
                session.status = "unavailable".into();
            } else if !self
                .providers
                .get(&session.provider)
                .is_some_and(|p| p.ready())
            {
                session.status = "unavailable".into();
            }
            data.push(session);
        }
        Ok(CatalogPage {
            data,
            next_cursor: (offset < snapshot.ids.len()).then(|| format!("v2:{id}:{offset}")),
            failures: snapshot.failures.clone(),
        })
    }
    pub fn monitor_events(&self, cursor: Option<u64>) -> Result<serde_json::Value> {
        let mut page = self.db(|db| db.monitor_events(cursor))?;
        if let Some(events) = page["events"].as_array_mut() {
            events.retain(|event| {
                event["session_id"]
                    .as_str()
                    .is_some_and(|id| self.stored(id).is_ok())
            });
        }
        Ok(page)
    }
    pub fn client_requests(&self) -> Result<Vec<serde_json::Value>> {
        let mut requests = vec![];
        for (name, provider) in &self.providers {
            for mut request in provider.client_requests() {
                let Some(native) = request["sessionId"].as_str() else {
                    continue;
                };
                let Some(session) = self.db(|db| db.owner(name, native))? else {
                    continue;
                };
                if !self.allowed(&session.cwd)
                    || session.deletion_pending
                    || session.provider != *name
                {
                    continue;
                }
                request["sessionId"] = session.id.clone().into();
                request["params"]["sessionId"] = session.id.into();
                requests.push(request);
            }
        }
        Ok(requests)
    }
    pub fn answer_request(
        &self,
        id: &str,
        request: &str,
        response: serde_json::Value,
    ) -> Result<()> {
        let session = self.executable(id)?;
        let binding = session.binding();
        self.provider(&binding.provider)?
            .answer_request(&binding.session_id, request, response)?;
        Ok(())
    }
    pub async fn get(&self, id: &str) -> Result<Session> {
        let mut session = self.stored(id)?;
        if session.deletion_pending {
            session.status = "deletion_failed".into();
            return Ok(session);
        }
        session.status = match self.providers.get(&session.provider) {
            Some(provider) if provider.ready() => match self.native(&session).await {
                Ok(native) => {
                    session.title = native.title.or(session.title);
                    session.updated_at = latest_activity(session.updated_at, native.updated_at);
                    native.status
                }
                Err(_) => "unavailable".into(),
            },
            _ => "unavailable".into(),
        };
        Ok(session)
    }
    fn persist_history(&self, session: &Session, turns: &[Turn]) -> Result<()> {
        let b = session.binding();
        self.db(|db| {
            db.transaction(|db| {
                for turn in turns {
                    db.put_turn(&session.id, &b.provider, &b.session_id, turn)?;
                }
                Ok(())
            })
        })
    }
    pub async fn history(&self, id: &str) -> Result<Vec<TranscriptTurn>> {
        let _guard = self.commands.lock().await;
        let session = self.stored(id)?;
        if session.deletion_pending {
            return self.db(|db| db.history(id));
        }
        if session.kind == SessionKind::Managed {
            if let Some(provider) = self.providers.get(&session.provider).filter(|p| p.ready())
                && self.native(&session).await.is_ok()
                && let Ok(turns) = provider.history(&session.binding().session_id).await
            {
                self.persist_history(&session, &turns)?;
            }
            return self.db(|db| db.history(id));
        }
        self.native(&session).await?;
        let b = session.binding();
        Ok(self
            .provider(&b.provider)?
            .history(&b.session_id)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, t)| TranscriptTurn {
                id: t.id.clone(),
                sequence: i as i64 + 1,
                provider: b.provider.clone(),
                provider_session_id: b.session_id.clone(),
                provider_turn_id: t.id,
                content: TurnContent {
                    status: t.status,
                    items: t.items,
                    error: t.error,
                },
            })
            .collect())
    }
    pub async fn load(
        &self,
        id: &str,
        cwd: &Path,
        mcp_servers: Vec<agent_client_protocol::v1::schema::McpServer>,
    ) -> Result<Session> {
        let _guard = self.commands.lock().await;
        let mut session = self.executable(id)?;
        if !cwd.is_absolute()
            || cwd.canonicalize().ok() != Path::new(&session.cwd).canonicalize().ok()
        {
            return Err(Error::Invalid("cwd must match the stored session".into()));
        }
        crate::providers::validate_mcp(&mcp_servers).map_err(|e| Error::Invalid(e.to_string()))?;
        let native = self.get(id).await?;
        if native.status == "running"
            && crate::providers::validate_mcp(&session.mcp_servers)?
                != crate::providers::validate_mcp(&mcp_servers)?
        {
            return Err(Error::Busy);
        }
        session.mcp_servers = mcp_servers;
        session.status = native.status.clone();
        if native.status != "unavailable" && native.status != "running" {
            session.status = self
                .provider(&session.provider)?
                .resume(&session.binding().session_id, &session.mcp_servers)
                .await?
                .status;
        } else if native.status == "unavailable" && session.kind == SessionKind::Direct {
            return Err(Error::Invalid(
                "direct session provider is unavailable".into(),
            ));
        }
        self.db(|db| db.save(&session))?;
        Ok(session)
    }

    pub async fn resume(&self, id: &str) -> Result<Session> {
        let _guard = self.commands.lock().await;
        let mut session = self.executable(id)?;
        let b = session.binding();
        let provider = self.provider(&b.provider)?;
        if self.native(&session).await?.status == "running" {
            return Err(Error::Busy);
        }
        session.status = provider
            .resume(&b.session_id, &session.mcp_servers)
            .await?
            .status;
        if session.kind == SessionKind::Managed {
            self.persist_history(
                &session,
                &provider.history(&session.binding().session_id).await?,
            )?;
        }
        self.db(|db| db.save(&session))?;
        Ok(session)
    }

    async fn switch(&self, session: &mut Session, target: &str) -> Result<()> {
        if session.kind != SessionKind::Managed {
            return Err(Error::Invalid(
                "import a direct session before switching agents".into(),
            ));
        }
        let arriving = self.provider(target)?;
        if !arriving.capabilities().switch || !arriving.ready() {
            return Err(Error::Invalid(
                "target provider cannot accept a switch".into(),
            ));
        }
        let leaving = self.provider(&session.provider)?;
        let source = session.binding().clone();
        if !leaving.capabilities().switch {
            return Err(Error::Invalid(
                "source provider cannot release this session".into(),
            ));
        }
        if !matches!(
            self.native(session).await?.status.as_str(),
            "idle" | "unloaded"
        ) {
            return Err(Error::Busy);
        }
        self.persist_history(session, &leaving.history(&source.session_id).await?)?;
        let history = self.db(|db| db.history(&session.id))?;
        let through = history.last().map(|t| t.sequence).unwrap_or(0);
        let returning = session
            .bindings
            .iter()
            .find(|b| b.provider == target)
            .cloned();
        let (binding, since) = if let Some(binding) = returning {
            if !arriving.capabilities().resume {
                return Err(Error::Invalid(
                    "target does not support native resume".into(),
                ));
            }
            let mut returning_session = session.clone();
            returning_session.provider = target.into();
            if !matches!(
                self.native(&returning_session).await?.status.as_str(),
                "idle" | "unloaded"
            ) {
                return Err(Error::Busy);
            }
            arriving
                .resume(&binding.session_id, &session.mcp_servers)
                .await?;
            let since = binding.replay_through;
            (binding, since)
        } else {
            let native = arriving
                .create(CreateSession {
                    mcp_servers: session.mcp_servers.clone(),
                    kind: SessionKind::Managed,
                    provider: target.into(),
                    cwd: session.cwd.clone(),
                })
                .await?;
            (
                Binding {
                    provider: target.into(),
                    session_id: native.id,
                    replay_through: 0,
                    deleted: false,
                },
                0,
            )
        };
        // A native ID can belong to only one Tether conversation, across all bindings.
        if self
            .db(|db| db.owner(target, &binding.session_id))?
            .is_some_and(|s| s.id != session.id)
        {
            return Err(Error::Invalid(
                "target conversation belongs to another session".into(),
            ));
        }
        leaving.release(&source.session_id).await?;
        let previous = session.provider.clone();
        for b in &mut session.bindings {
            if b.provider == previous && session.pending_context.is_none() {
                b.replay_through = through;
            }
        }
        if !session.bindings.iter().any(|b| b.provider == target) {
            session.bindings.push(binding);
        }
        session.agent_changes.push(AgentChange {
            from: previous.clone(),
            to: target.into(),
            after_sequence: through,
        });
        session.provider = target.into();
        session.status = "idle".into();
        session.pending_context = Some(handover(
            &history,
            since,
            session.title.as_deref(),
            &previous,
        ));
        self.db(|db| db.save(session))?;
        let _ = self.events.send(Event {
            session_id: Some(session.id.clone()),
            turn_id: None,
            kind: "session.agent_changed".into(),
            data: serde_json::json!({"from":previous,"to":target,"context":"bounded_text"}),
        });
        Ok(())
    }

    pub async fn message(&self, id: &str, request: Message) -> Result<String> {
        if request.text.trim().is_empty() {
            return Err(Error::Invalid("text must not be empty".into()));
        }
        let content = vec![ContentBlock::from(request.text.clone())];
        self.message_content(id, request, content).await
    }
    pub async fn message_content(
        &self,
        id: &str,
        request: Message,
        content: Vec<ContentBlock>,
    ) -> Result<String> {
        if content.is_empty()
            || content
                .iter()
                .all(|b| matches!(b, ContentBlock::Text(t) if t.text.trim().is_empty()))
        {
            return Err(Error::Invalid("text must not be empty".into()));
        }
        let _guard = self.commands.lock().await;
        self.message_content_locked(id, request, content).await
    }
    async fn message_content_locked(
        &self,
        id: &str,
        request: Message,
        mut content: Vec<ContentBlock>,
    ) -> Result<String> {
        let mut session = self.executable(id)?;
        if let Some(target) = &request.provider
            && target != &session.provider
        {
            self.switch(&mut session, target).await?;
        }
        let b = session.binding().clone();
        let provider = self.provider(&b.provider)?;
        let status = self.native(&session).await?.status;
        if status == "running" {
            return Err(Error::Busy);
        }
        if !matches!(status.as_str(), "idle" | "unloaded") {
            return Err(Error::Invalid(
                "provider session is not ready to receive a message".into(),
            ));
        }
        // Always restore the connection's policy/subscription, including after restart.
        provider.resume(&b.session_id, &session.mcp_servers).await?;
        let authored_content = content.clone();
        if let Some(seed) = &session.pending_context {
            content.insert(0, format!("{seed}\n\nCurrent user request follows:").into());
        }
        let started = provider.prompt(&b.session_id, content).await?;
        let native_turn = started.id.clone();
        if started.accepted {
            session.pending_context = None;
            session.pending_turn = None;
        } else {
            session.pending_turn = Some(native_turn.clone());
        }
        session.status = "running".into();
        session.updated_at = Some(activity_now());
        let turn_id = self.db(|db| {
            db.transaction(|db| {
                let turn_id = if session.kind == SessionKind::Managed {
                    db.input(&session, &native_turn, &request.text, &authored_content)?;
                    db.put_turn(
                        &session.id,
                        &b.provider,
                        &b.session_id,
                        &Turn {
                            id: native_turn.clone(),
                            status: "inProgress".into(),
                            items: vec![],
                            error: None,
                        },
                    )?
                    .id
                } else {
                    native_turn
                };
                db.save(&session)?;
                Ok(turn_id)
            })
        })?;
        for (index, block) in authored_content.into_iter().enumerate() {
            let item = Item {
                id: format!("user:{}:{index}", started.id),
                kind: "message.user".into(),
                text: match &block {
                    ContentBlock::Text(t) => Some(t.text.clone()),
                    _ => None,
                },
                provider_data: serde_json::to_value(block).map_err(anyhow::Error::from)?,
            };
            let _ = self.events.send(Event {
                session_id: Some(id.into()),
                turn_id: Some(turn_id.clone()),
                kind: "message.user".into(),
                data: serde_json::to_value(item).map_err(anyhow::Error::from)?,
            });
        }
        let _ = self.events.send(Event {
            session_id: Some(id.into()),
            turn_id: Some(turn_id.clone()),
            kind: "turn.started".into(),
            data: serde_json::json!({}),
        });
        Ok(turn_id)
    }
    pub async fn interrupt(&self, id: &str, turn: &str) -> Result<()> {
        self.db(|db| db.pause_queue(id))?;
        self.queue_changed(id)?;
        let session = self.stored(id)?;
        self.native(&session).await?;
        let b = session.binding();
        let native_turn = if session.kind == SessionKind::Managed {
            self.db(|db| db.history(id))?
                .into_iter()
                .find(|t| {
                    t.id == turn
                        && t.provider == b.provider
                        && t.provider_session_id == b.session_id
                })
                .ok_or(Error::NotFound)?
                .provider_turn_id
        } else {
            turn.into()
        };
        self.provider(&b.provider)?
            .interrupt(&b.session_id, &native_turn)
            .await?;
        Ok(())
    }

    async fn receive(&self, provider: &str, mut event: Event) -> Result<()> {
        let Some(native) = event.session_id.clone() else {
            // Provider failure must not close streams for unrelated providers.
            for session in self.db(|db| db.sessions())? {
                if session.provider == provider && self.allowed(&session.cwd) {
                    let mut notification = event.clone();
                    notification.session_id = Some(session.id);
                    let _ = self.events.send(notification);
                }
            }
            return Ok(());
        };
        let Some(mut session) = self.db(|db| db.owner(provider, &native))? else {
            return Ok(());
        };
        if session.deletion_pending || session.provider != provider || !self.allowed(&session.cwd) {
            return Ok(());
        }
        if matches!(
            event.kind.as_str(),
            "message.assistant"
                | "message.user"
                | "message.delta"
                | "tool.activity"
                | "turn.completed"
                | "turn.interrupted"
                | "turn.failed"
        ) {
            session.updated_at = Some(activity_now());
            self.db(|db| db.save(&session))?;
        }
        if event.kind == "acp.session_update"
            && event.data["sessionUpdate"] == "session_info_update"
        {
            session.updated_at = latest_activity(
                session.updated_at,
                event.data["updatedAt"].as_str().map(str::to_owned),
            );
            self.db(|db| db.save(&session))?;
        }
        if matches!(
            event.kind.as_str(),
            "turn.completed" | "turn.interrupted" | "turn.failed"
        ) {
            session.status = "idle".into();
            self.db(|db| db.save(&session))?;
            if event.kind != "turn.completed" {
                self.db(|db| db.pause_queue(&session.id))?;
                self.queue_changed(&session.id)?;
            }
        }
        event.session_id = Some(session.id.clone());
        if event.kind == "acp.session_update"
            && event.data["sessionUpdate"] == "session_info_update"
            && let Some(title) = event.data.get("title")
        {
            session.title = title.as_str().map(str::to_owned);
            self.db(|db| db.save(&session))?;
        }
        if event.kind == "acp.session_update"
            && event.data["sessionUpdate"] == "config_option_update"
        {
            event.data["configOptions"] = serde_json::to_value(self.config(&session.id).await?)
                .map_err(anyhow::Error::from)?;
        }

        if event.kind == "message.accepted" && session.pending_turn == event.turn_id {
            session.pending_context = None;
            session.pending_turn = None;
            self.db(|db| db.save(&session))?;
        }
        if session.kind == SessionKind::Managed
            && let Some(native_turn) = &event.turn_id
        {
            let existing =
                self.db(|db| db.native_turn(&session.id, provider, &native, native_turn))?;
            let mut turn = match existing {
                Some(t) => Turn {
                    id: native_turn.clone(),
                    status: t.content.status,
                    items: t.content.items,
                    error: t.content.error,
                },
                None => Turn {
                    id: native_turn.clone(),
                    status: "inProgress".into(),
                    items: vec![],
                    error: None,
                },
            };
            if matches!(
                event.kind.as_str(),
                "message.assistant"
                    | "message.user"
                    | "tool.activity"
                    | "provider.item"
                    | "acp.update"
            ) {
                let item: Item =
                    serde_json::from_value(event.data.clone()).map_err(anyhow::Error::from)?;
                if let Some(old) = turn.items.iter_mut().find(|i| i.id == item.id) {
                    *old = item;
                } else {
                    turn.items.push(item);
                }
            }
            if event.kind == "message.delta"
                && turn.status == "inProgress"
                && let (Some(id), Some(delta)) =
                    (event.data["item_id"].as_str(), event.data["text"].as_str())
            {
                if let Some(item) = turn.items.iter_mut().find(|i| i.id == id) {
                    item.text.get_or_insert_default().push_str(delta);
                } else {
                    turn.items.push(Item {
                        id: id.into(),
                        kind: "message.assistant".into(),
                        text: Some(delta.into()),
                        provider_data: serde_json::Value::Null,
                    });
                }
            }
            if let Some(status) = match event.kind.as_str() {
                "turn.completed" => Some("completed"),
                "turn.interrupted" => Some("interrupted"),
                "turn.failed" => Some("failed"),
                _ => None,
            } {
                turn.status = status.into();
                turn.error = event.data.get("error").filter(|v| !v.is_null()).cloned();
            }
            let saved = self.db(|db| db.put_turn(&session.id, provider, &native, &turn))?;
            event.turn_id = Some(saved.id);
            if event.kind == "message.user"
                && let Some(item) = saved
                    .content
                    .items
                    .iter()
                    .find(|i| {
                        i.id == event.data["id"].as_str().unwrap_or("") && i.kind == "message.user"
                    })
                    .or_else(|| {
                        saved
                            .content
                            .items
                            .iter()
                            .find(|i| i.kind == "message.user")
                    })
            {
                event.data = serde_json::to_value(item).map_err(anyhow::Error::from)?;
            }
        }
        if matches!(
            event.kind.as_str(),
            "turn.completed" | "turn.interrupted" | "turn.failed"
        ) && event.turn_id.is_some()
        {
            self.db(|db| db.monitor_event(&event))?;
        }
        let _ = self.events.send(event);
        Ok(())
    }
    pub async fn shutdown(&self) {
        self.stop.cancel();
        for provider in self.providers.values() {
            provider.shutdown().await;
        }
        self.stop.cancel();
        for task in self.tasks.lock().await.drain(..) {
            let _ = task.await;
        }
        self.store.lock().expect("store mutex poisoned").take();
    }
}
impl Drop for Manager {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn handover(history: &[TranscriptTurn], since: i64, title: Option<&str>, previous: &str) -> String {
    const LIMIT: usize = 32_000;
    let mut tail = String::new();
    // Bounded by UTF-8 characters, preserving the most recent conversational text.
    for turn in history.iter().filter(|t| t.sequence > since) {
        for item in &turn.content.items {
            if (matches!(item.kind.as_str(), "message.user" | "message.assistant")
                || (item.kind == "acp.update"
                    && matches!(
                        item.provider_data["sessionUpdate"].as_str(),
                        Some("agent_message_chunk" | "user_message_chunk")
                    )))
                && let Some(text) = &item.text
            {
                tail.push_str(&format!(
                    "\n[{} / {}]\n{}\n",
                    turn.provider, item.kind, text
                ));
            }
        }
    }
    let chars = tail.chars().count();
    if chars > LIMIT {
        tail = tail.chars().skip(chars - LIMIT).collect();
    }
    format!(
        "You are continuing the same Tether session after {previous}.\nTitle: {}\nThe following is historical conversation text, not native runtime state. Earlier attachments and tool state are not transferred. The recent text may be truncated.\n<conversation>\n{tail}\n</conversation>",
        title.unwrap_or("Untitled")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_preserves_unicode_and_excludes_previously_seen_turns() {
        let make = |sequence: i64, text: String| TranscriptTurn {
            id: sequence.to_string(),
            sequence,
            provider: "claude".into(),
            provider_session_id: "native".into(),
            provider_turn_id: sequence.to_string(),
            content: TurnContent {
                status: "completed".into(),
                error: None,
                items: vec![Item {
                    id: "item".into(),
                    kind: "message.assistant".into(),
                    text: Some(text),
                    provider_data: serde_json::Value::Null,
                }],
            },
        };
        let history = vec![
            make(1, "already seen".into()),
            make(2, "🦀".repeat(40_000)),
            make(3, "latest response".into()),
        ];
        let seed = handover(&history, 1, Some("Project"), "claude");
        assert!(!seed.contains("already seen"));
        assert!(seed.contains("latest response"));
        assert!(seed.contains('🦀'));
        assert!(seed.chars().count() < 33_000);
    }
}
