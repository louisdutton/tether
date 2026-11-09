use anyhow::{Context, Result, ensure};
use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};
use tether::{
    acp,
    api::{self, AppState},
    manager::Manager,
    providers::{
        Provider,
        acp::{AcpProvider, AgentCommand},
    },
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tether=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let root = PathBuf::from(
        env::var("TETHER_WORKSPACE_ROOT")
            .context("set TETHER_WORKSPACE_ROOT to the directory containing your projects")?,
    )
    .canonicalize()?;
    ensure!(root.is_dir(), "workspace root must be a directory");
    let arguments: Vec<_> = env::args().skip(1).collect();
    ensure!(
        arguments.is_empty() || arguments == ["--stdio"],
        "usage: tether [--stdio]"
    );
    let stdio = !arguments.is_empty();
    let database = env::var("TETHER_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            env::var("XDG_DATA_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    PathBuf::from(env::var("HOME").expect("HOME or TETHER_DATA_DIR is required"))
                        .join(".local/share")
                })
                .join("tether")
        })
        .join("sessions.sqlite3");
    let agents: Vec<AgentCommand> = if let Ok(value) = env::var("TETHER_AGENTS") {
        serde_json::from_str(&value)
            .context("TETHER_AGENTS must be an array of {id,command,args}")?
    } else {
        let mut agents = vec![AgentCommand {
            id: "codex".into(),
            command: env::var("TETHER_CODEX_ACP_BIN")
                .unwrap_or_else(|_| "codex-acp".into())
                .into(),
            args: vec![],
        }];
        if let Ok(binary) = env::var("TETHER_CLAUDE_ACP_BIN") {
            agents.push(AgentCommand {
                id: "claude".into(),
                command: binary.into(),
                args: vec![],
            });
        }
        agents
    };
    let mut providers: Vec<Arc<dyn Provider>> = Vec::new();
    for agent in agents {
        providers.push(Arc::new(
            AcpProvider::connect(
                agent,
                &database
                    .parent()
                    .expect("database directory")
                    .join("agents"),
            )
            .await?,
        ));
    }
    let manager = Manager::open(root, &database, providers).await?;
    let shutdown = CancellationToken::new();
    let outcome = if stdio {
        tokio::select! {
            result = acp::stdio(manager.clone(), shutdown.clone()) => result.map_err(anyhow::Error::from),
            _ = signal() => Ok(()),
        }
    } else {
        let addr: SocketAddr = env::var("TETHER_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8787".into())
            .parse()?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let state = AppState {
            manager: manager.clone(),
            shutdown: shutdown.clone(),
        };
        tracing::info!(address = %listener.local_addr()?, "Tether ACP WebSocket listening");
        tokio::select! {
            result = axum::serve(listener, api::router(state)).with_graceful_shutdown(shutdown.clone().cancelled_owned()) => result.map_err(anyhow::Error::from),
            _ = signal() => Ok(()),
        }
    };
    shutdown.cancel();
    manager.shutdown().await;
    outcome
}

async fn signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
}
