use super::*;
use serde_json::{Value, json};
use std::time::Duration;

impl Manager {
    pub fn supports_steering(&self) -> bool {
        self.providers.values().all(|p| p.supports_steering())
    }
    pub fn queued_messages(&self, id: &str) -> Result<Vec<QueuedMessage>> {
        self.stored(id)?;
        Ok(self
            .db(|db| db.queued(Some(id)))?
            .into_iter()
            .filter(|m| !matches!(m.state.as_str(), "delivered" | "cancelled"))
            .collect())
    }
    pub(super) fn queue_changed(&self, id: &str) -> Result<()> {
        let messages = self.queued_messages(id)?;
        let _ = self.events.send(Event {
            session_id: Some(id.into()),
            turn_id: None,
            kind: "queue.updated".into(),
            data: json!({"messages":messages}),
        });
        Ok(())
    }
    pub async fn enqueue(
        &self,
        id: &str,
        message_id: String,
        prompt: Vec<ContentBlock>,
    ) -> Result<QueuedMessage> {
        let _guard = self.commands.lock().await;
        self.executable(id)?;
        validate_message(&prompt)?;
        if message_id.len() > 128 || message_id.is_empty() {
            return Err(Error::Invalid("invalid message id".into()));
        }
        if let Some(old) = self
            .db(|db| db.queued(None))?
            .into_iter()
            .find(|m| m.message_id == message_id)
        {
            if old.session_id != id || old.prompt != prompt {
                return Err(Error::Invalid(
                    "message id already used for different input".into(),
                ));
            }
            return Ok(old);
        }
        if self.queued_messages(id)?.len() >= 100 {
            return Err(Error::Invalid("session queue is full".into()));
        }
        let message = QueuedMessage {
            message_id,
            session_id: id.into(),
            prompt,
            state: "queued".into(),
            error: None,
        };
        self.db(|db| db.queue_message(&message))?;
        self.queue_changed(id)?;
        Ok(message)
    }
    pub async fn change_queued_message(
        &self,
        id: &str,
        message_id: &str,
        retry: bool,
    ) -> Result<()> {
        let _guard = self.commands.lock().await;
        self.executable(id)?;
        let message = self
            .queued_messages(id)?
            .into_iter()
            .find(|m| m.message_id == message_id)
            .ok_or(Error::NotFound)?;
        if message.state == "dispatching" {
            return Err(Error::Busy);
        }
        if retry && message.state != "paused" {
            return Err(Error::Invalid(
                "only paused messages can be resumed; check uncertain delivery before resending"
                    .into(),
            ));
        }
        self.db(|db| db.queue_state(message_id, if retry { "queued" } else { "cancelled" }, None))?;
        self.queue_changed(id)
    }
    pub async fn steer(&self, id: &str, prompt: Vec<ContentBlock>) -> Result<Value> {
        validate_message(&prompt)?;
        let _guard = self.commands.lock().await;
        let session = self.executable(id)?;
        let provider = self.provider(&session.provider)?;
        if !provider.supports_steering() {
            return Err(Error::Invalid(
                "agent does not support live steering".into(),
            ));
        }
        if self.native(&session).await?.status != "running" {
            return Ok(json!({"outcome":"promptRequired","reason":"noRunningTurn"}));
        }
        if provider
            .steer(&session.binding().session_id, prompt)
            .await?
        {
            Ok(json!({"outcome":"injected"}))
        } else {
            Ok(json!({"outcome":"promptRequired","reason":"noRunningTurn"}))
        }
    }
    pub(super) async fn start_queue_worker(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let stop = self.stop.clone();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! { _=stop.cancelled()=>break, _=tick.tick()=>{} }
                let Some(manager) = weak.upgrade() else { break };
                if let Err(error) = manager.dispatch_queued().await {
                    tracing::error!(?error, "queued delivery failed");
                }
            }
        });
        self.tasks.lock().await.push(task);
    }
    async fn dispatch_queued(&self) -> Result<()> {
        let _guard = self.commands.lock().await;
        let messages = self.db(|db| db.queued(None))?;
        let mut visited = std::collections::HashSet::new();
        for message in messages
            .into_iter()
            .filter(|m| !matches!(m.state.as_str(), "delivered" | "cancelled"))
        {
            // A paused/uncertain head blocks subsequent messages for this session.
            if !visited.insert(message.session_id.clone()) || message.state != "queued" {
                continue;
            }
            let id = &message.session_id;
            let Ok(session) = self.executable(id) else {
                continue;
            };
            if session.status == "running" || !self.provider(&session.provider)?.ready() {
                continue;
            }
            let Ok(native) = self.native(&session).await else {
                continue;
            };
            if !matches!(native.status.as_str(), "idle" | "unloaded") {
                continue;
            }
            self.db(|db| db.queue_state(&message.message_id, "dispatching", None))?;
            self.queue_changed(id)?;
            let text = message
                .prompt
                .iter()
                .filter_map(|p| match p {
                    ContentBlock::Text(t) => Some(t.text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            let result = self
                .message_content_locked(
                    id,
                    Message {
                        provider: None,
                        text,
                    },
                    message.prompt,
                )
                .await;
            match result {
                Ok(_) => self.db(|db| db.queue_state(&message.message_id, "delivered", None))?,
                Err(Error::Busy) => {
                    self.db(|db| db.queue_state(&message.message_id, "queued", None))?
                }
                Err(error) => self.db(|db| {
                    db.queue_state(
                        &message.message_id,
                        "uncertain",
                        Some(&format!("Delivery could not be confirmed: {error:?}")),
                    )
                })?,
            }
            self.queue_changed(id)?;
        }
        Ok(())
    }
}
fn validate_message(prompt: &[ContentBlock]) -> Result<()> {
    if prompt.is_empty()
        || prompt
            .iter()
            .all(|p| matches!(p,ContentBlock::Text(t) if t.text.trim().is_empty()))
    {
        return Err(Error::Invalid("message must not be empty".into()));
    }
    Ok(())
}
