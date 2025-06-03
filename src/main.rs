use std::{collections::HashMap, net::SocketAddr, time::Duration};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use futures::StreamExt;
use serde_json;
use tokio_postgres::{NoTls, Client};
use uuid::Uuid;
use warp::Filter;

#[derive(Clone)]
struct AppState {
    sessions: Arc<Mutex<HashMap<Uuid, Session>>>,
}

use std::sync::Arc;

struct Session {
    client: Client,
    _bg_task: tokio::task::JoinHandle<()>,
}

#[derive(Deserialize)]
#[serde(tag = "action")]
enum WsRequest {
    #[serde(rename = "connect")]
    Connect { user: String, password: String },
    #[serde(rename = "disconnect")]
    Disconnect { session_id: Uuid },
}

#[derive(Serialize)]
struct WsResponse {
    status: String,
    session_id: Option<Uuid>,
    error: Option<String>,
}

const SESSION_TIMEOUT: Duration = Duration::from_secs(60 * 10); // 10 minutes

#[tokio::main]
async fn main() {
    let state = AppState {
        sessions: Arc::new(Mutex::new(HashMap::new())),
    };

    let state_filter = warp::any().map(move || state.clone());

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(state_filter)
        .map(|ws: warp::ws::Ws, state: AppState| {
            ws.on_upgrade(move |socket| client_connection(socket, state))
        });

    let addr: SocketAddr = ([0, 0, 0, 0], 3030).into();
    println!("Listening on {}", addr);
    warp::serve(ws_route).run(addr).await;
}

async fn client_connection(ws: warp::ws::WebSocket, state: AppState) {
    let (mut tx, mut rx) = ws.split();
    while let Some(result) = rx.next().await {
        if let Ok(msg) = result {
            if msg.is_text() {
                if let Ok(req) = serde_json::from_str::<WsRequest>(msg.to_str().unwrap_or("")) {
                    match req {
                        WsRequest::Connect { user, password } => {
                            let response = match create_session(&state, &user, &password).await {
                                Ok(id) => WsResponse { status: "ok".into(), session_id: Some(id), error: None },
                                Err(e) => WsResponse { status: "error".into(), session_id: None, error: Some(e) },
                            };
                            let _ = tx.send(warp::ws::Message::text(serde_json::to_string(&response).unwrap())).await;
                        }
                        WsRequest::Disconnect { session_id } => {
                            destroy_session(&state, &session_id).await;
                            let response = WsResponse { status: "ok".into(), session_id: None, error: None };
                            let _ = tx.send(warp::ws::Message::text(serde_json::to_string(&response).unwrap())).await;
                        }
                    }
                }
            }
        } else {
            break;
        }
    }
}

async fn create_session(state: &AppState, user: &str, password: &str) -> Result<Uuid, String> {
    let conn_str = format!("host=localhost user={} password={}", user, password);
    match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok((client, connection)) => {
            let session_id = Uuid::new_v4();
            let task = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            state.sessions.lock().insert(session_id, Session { client, _bg_task: task });

            let cleanup_sessions = state.sessions.clone();
            tokio::spawn(async move {
                tokio::time::sleep(SESSION_TIMEOUT).await;
                cleanup_sessions.lock().remove(&session_id);
            });
            Ok(session_id)
        }
        Err(e) => Err(e.to_string()),
    }
}

async fn destroy_session(state: &AppState, session_id: &Uuid) {
    if let Some(session) = state.sessions.lock().remove(session_id) {
        let _ = session.client.close().await;
        session._bg_task.abort();
    }
}
