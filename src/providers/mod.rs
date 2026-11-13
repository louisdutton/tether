pub mod acp;
pub mod requests;
use agent_client_protocol::v1::schema::{
    ContentBlock, McpCapabilities, PromptCapabilities, SessionConfigOption,
    SessionConfigOptionValue, SessionModeId, SessionModeState, SessionUpdate,
};

use crate::model::{Capabilities, CreateSession, Event, Page, ProviderSession, StartedTurn, Turn};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;

// Only operations needed by the pilot; wire formats stay inside each adapter.
#[async_trait]
pub trait Provider: Send + Sync {
    fn client_requests(&self) -> Vec<serde_json::Value> {
        vec![]
    }
    fn answer_request(
        &self,
        _session: &str,
        _id: &str,
        _response: serde_json::Value,
    ) -> Result<()> {
        anyhow::bail!("request no longer available")
    }
    fn supports_steering(&self) -> bool {
        false
    }
    async fn steer(&self, _id: &str, _content: Vec<ContentBlock>) -> Result<bool> {
        anyhow::bail!("agent does not support steering")
    }
    fn supports_delete(&self) -> bool;
    async fn delete(&self, id: &str) -> Result<()>;
    fn supports_close(&self) -> bool;
    async fn close(&self, id: &str) -> Result<Vec<Turn>>;
    async fn modes(&self, id: &str) -> Result<Option<SessionModeState>>;
    async fn set_mode(&self, id: &str, mode: SessionModeId) -> Result<()>;
    async fn session_updates(&self, id: &str) -> Result<Vec<SessionUpdate>>;
    fn id(&self) -> &str;
    fn mcp_capabilities(&self) -> McpCapabilities;
    async fn managed(&self, id: &str) -> Result<()>;
    fn prompt_capabilities(&self) -> PromptCapabilities;
    async fn config(&self, id: &str) -> Result<Vec<SessionConfigOption>>;
    async fn set_config(
        &self,
        id: &str,
        key: &str,
        value: SessionConfigOptionValue,
    ) -> Result<Vec<SessionConfigOption>>;
    async fn prompt(&self, id: &str, content: Vec<ContentBlock>) -> Result<StartedTurn>;
    fn ready(&self) -> bool;
    fn capabilities(&self) -> Capabilities;
    async fn release(&self, id: &str) -> Result<()>;
    fn subscribe(&self) -> broadcast::Receiver<Event>;
    async fn create(&self, request: CreateSession) -> Result<ProviderSession>;
    async fn list(&self, cursor: Option<String>) -> Result<Page<ProviderSession>>;
    async fn get(&self, id: &str) -> Result<ProviderSession>;
    async fn history(&self, id: &str) -> Result<Vec<Turn>>;
    async fn resume(
        &self,
        id: &str,
        mcp_servers: &[agent_client_protocol::v1::schema::McpServer],
    ) -> Result<ProviderSession>;
    async fn interrupt(&self, id: &str, turn: &str) -> Result<()>;
    async fn shutdown(&self);
}

// Paths in ACP tool output are absolute even when an adapter reports them
// relative to its session. Leave opaque raw input/output and metadata untouched.
pub(crate) fn absolute_update_paths(update: &mut SessionUpdate, cwd: &std::path::Path) {
    use agent_client_protocol::v1::schema::{ToolCallContent, ToolCallLocation};
    fn paths(
        content: &mut [ToolCallContent],
        locations: &mut [ToolCallLocation],
        cwd: &std::path::Path,
    ) {
        for location in locations {
            if !location.path.is_absolute() {
                location.path = cwd.join(&location.path);
            }
        }
        for content in content {
            if let ToolCallContent::Diff(diff) = content
                && !diff.path.is_absolute()
            {
                diff.path = cwd.join(&diff.path);
            }
        }
    }
    match update {
        SessionUpdate::ToolCall(tool) => paths(&mut tool.content, &mut tool.locations, cwd),
        SessionUpdate::ToolCallUpdate(tool) => paths(
            tool.fields.content.as_deref_mut().unwrap_or_default(),
            tool.fields.locations.as_deref_mut().unwrap_or_default(),
            cwd,
        ),
        _ => {}
    }
}

// Validate the standard types, without translating to a provider-native format.
pub fn validate_mcp(
    servers: &[agent_client_protocol::v1::schema::McpServer],
) -> anyhow::Result<serde_json::Value> {
    use agent_client_protocol::v1::schema::McpServer;
    let mut names = std::collections::HashSet::new();
    for server in servers {
        let name = match server {
            McpServer::Stdio(s) => {
                anyhow::ensure!(s.command.is_absolute(), "MCP command must be absolute");
                &s.name
            }
            McpServer::Http(s) => &s.name,
            McpServer::Sse(s) => &s.name,
            _ => anyhow::bail!("unsupported MCP transport"),
        };
        anyhow::ensure!(
            !name.is_empty() && names.insert(name),
            "MCP names must be nonempty and unique"
        );
    }
    Ok(serde_json::to_value(servers)?)
}
