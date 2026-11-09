//! Remote ACP transport. The WebSocket framing is Tether-specific; messages are ACP v1.
use crate::{acp, manager::Manager};
use axum::{
    Json, Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::Response,
    routing::get,
};
use futures::{SinkExt, StreamExt};
use serde_json::json;
use std::{io, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<Manager>,
    pub shutdown: CancellationToken,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/acp", get(upgrade))
        .with_state(state)
}
async fn health(State(state): State<AppState>) -> impl axum::response::IntoResponse {
    if state.manager.ready() {
        (StatusCode::OK, Json(json!({"status":"ok"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"provider_unavailable"})),
        )
    }
}
async fn upgrade(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| connection(socket, state))
}
async fn connection(socket: WebSocket, state: AppState) {
    let (writer, reader) = socket.split();
    let outgoing = writer.sink_map_err(io::Error::other).with(|line: String| {
        futures::future::ready(Ok::<_, io::Error>(Message::Text(line.into())))
    });
    let incoming = reader.filter_map(|message| {
        futures::future::ready(match message {
            Ok(Message::Text(text)) => Some(Ok(text.to_string())),
            Ok(Message::Binary(_)) => Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACP requires text WebSocket messages",
            ))),
            Ok(_) => None,
            Err(error) => Some(Err(io::Error::other(error))),
        })
    });
    if let Err(error) = acp::serve(
        agent_client_protocol::Lines::new(outgoing, incoming),
        state.manager,
        state.shutdown,
    )
    .await
    {
        tracing::debug!(%error, "ACP WebSocket connection ended");
    }
}
