use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderSession {
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub internal: bool,
    pub id: String,
    pub provider: String,
    pub cwd: String,
    pub status: String,
    pub title: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Page<T> {
    pub data: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Item {
    pub id: String,
    pub kind: String,
    pub text: Option<String>,
    pub provider_data: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Turn {
    pub id: String,
    pub status: String,
    pub items: Vec<Item>,
    pub error: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub session_id: Option<String>,
    pub turn_id: Option<String>,
    pub kind: String,
    pub data: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSession {
    #[serde(default)]
    pub mcp_servers: Vec<agent_client_protocol::v1::schema::McpServer>,
    #[serde(default)]
    pub kind: SessionKind,
    pub provider: String,
    pub cwd: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub provider: Option<String>,
    pub text: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pagination {
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interrupt {
    pub turn_id: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    #[default]
    Managed,
    Direct,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Binding {
    pub provider: String,
    pub session_id: String,
    pub replay_through: i64,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub deletion_pending: bool,
    #[serde(default)]
    pub deletion_error: Option<String>,
    #[serde(default, skip_serializing)]
    pub mcp_servers: Vec<agent_client_protocol::v1::schema::McpServer>,
    pub id: String,
    pub kind: SessionKind,
    pub machine_id: String,
    pub provider: String,
    pub cwd: String,
    pub status: String,
    pub title: Option<String>,
    pub bindings: Vec<Binding>,
    #[serde(default)]
    pub agent_changes: Vec<AgentChange>,
    #[serde(skip_serializing)]
    pub pending_context: Option<String>,
    #[serde(skip_serializing, default)]
    pub pending_turn: Option<String>,
}

impl Session {
    pub fn binding(&self) -> &Binding {
        self.bindings
            .iter()
            .find(|b| b.provider == self.provider)
            .expect("persisted session must have an active binding")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TranscriptTurn {
    pub id: String,
    pub sequence: i64,
    pub provider: String,
    pub provider_session_id: String,
    pub provider_turn_id: String,
    #[serde(flatten)]
    pub content: TurnContent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TurnContent {
    pub status: String,
    pub items: Vec<Item>,
    pub error: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptSession {
    pub provider: String,
    pub provider_session_id: String,
    #[serde(default)]
    pub kind: SessionKind,
}

#[derive(Debug, Serialize)]
pub struct ProviderInfo {
    pub id: String,
    pub ready: bool,
    pub capabilities: Capabilities,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Capabilities {
    pub delete: bool,
    pub resume: bool,
    pub list: bool,
    pub history: bool,
    pub interrupt: bool,
    pub switch: bool,
}

#[derive(Debug)]
pub struct StartedTurn {
    pub id: String,
    pub accepted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentChange {
    pub from: String,
    pub to: String,
    pub after_sequence: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogFailure {
    pub provider: String,
    pub message: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct CatalogPage {
    pub data: Vec<Session>,
    pub next_cursor: Option<String>,
    pub failures: Vec<CatalogFailure>,
}

/// Compare actual instants, including provider timestamps with differing offsets.
pub fn activity_timestamp(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|v| v.timestamp_millis())
}
pub fn latest_activity(a: Option<String>, b: Option<String>) -> Option<String> {
    let a = a.filter(|v| activity_timestamp(v).is_some());
    let b = b.filter(|v| activity_timestamp(v).is_some());
    if b.as_deref().and_then(activity_timestamp) > a.as_deref().and_then(activity_timestamp) {
        b
    } else {
        a
    }
}
pub fn activity_now() -> String {
    chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[derive(Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub ids: Vec<String>,
    pub cwd: Option<String>,
    pub failures: Vec<CatalogFailure>,
}
