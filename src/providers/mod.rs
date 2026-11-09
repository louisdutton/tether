pub mod acp;
use agent_client_protocol::v1::schema::{
    ContentBlock, McpCapabilities, PromptCapabilities, SessionConfigOption,
    SessionConfigOptionValue, SessionUpdate,
};

use crate::model::{Capabilities, CreateSession, Event, Page, ProviderSession, StartedTurn, Turn};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;

// Only operations needed by the pilot; wire formats stay inside each adapter.
#[async_trait]
pub trait Provider: Send + Sync {
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
