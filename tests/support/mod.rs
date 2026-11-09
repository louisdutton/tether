#![allow(dead_code)]
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};
use tempfile::TempDir;
use tether::{
    manager::Manager,
    model::{CreateSession, Message, Session, SessionKind, TranscriptTurn},
    providers::{
        Provider,
        acp::{AcpProvider, AgentCommand},
    },
};

pub fn scripts() -> &'static TempDir {
    static SCRIPTS: OnceLock<TempDir> = OnceLock::new();
    SCRIPTS.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acp");
        std::fs::write(&path, include_str!("../fixtures/acp")).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        dir
    })
}
pub fn command(directory: &Path, id: &str) -> AgentCommand {
    AgentCommand {
        id: id.into(),
        command: scripts().path().join("acp"),
        args: vec![id.into(), directory.join(id).to_string_lossy().into()],
    }
}
pub struct Fixture {
    pub directory: TempDir,
    pub manager: Arc<Manager>,
}
impl Fixture {
    pub async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let manager = Self::connect(directory.path()).await;
        Self { directory, manager }
    }
    pub async fn connect(directory: &Path) -> Arc<Manager> {
        let mut providers: Vec<Arc<dyn Provider>> = vec![];
        for id in ["codex", "claude"] {
            providers.push(Arc::new(
                AcpProvider::connect(command(directory, id), &directory.join("state/agents"))
                    .await
                    .unwrap(),
            ));
        }
        Manager::open(
            directory.canonicalize().unwrap(),
            &directory.join("state/sessions.sqlite3"),
            providers,
        )
        .await
        .unwrap()
    }
    pub async fn create(&self, provider: &str, kind: SessionKind) -> Session {
        self.manager
            .create(CreateSession {
                provider: provider.into(),
                kind,
                cwd: self.directory.path().to_string_lossy().into(),
                mcp_servers: vec![],
            })
            .await
            .unwrap()
    }
    pub async fn send(&self, id: &str, provider: Option<&str>, text: &str) -> String {
        self.manager
            .message(
                id,
                Message {
                    provider: provider.map(str::to_owned),
                    text: text.into(),
                },
            )
            .await
            .unwrap()
    }
    pub async fn finished(&self, id: &str, count: usize) -> Vec<TranscriptTurn> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let turns = self.manager.history(id).await.unwrap();
                if turns.len() >= count && turns.last().unwrap().content.status != "inProgress" {
                    return turns;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    pub fn requests(&self, id: &str) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.directory.path().join(id).join("requests.jsonl"))
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
    pub async fn close(&self) {
        self.manager.shutdown().await;
    }
}
pub fn data_dir(directory: &Path) -> PathBuf {
    directory.join("state")
}
