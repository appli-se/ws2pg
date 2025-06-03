use std::{collections::HashMap, fs::File, net::SocketAddr, time::Duration};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_yaml;
use futures::StreamExt;
use serde_json;
use tokio_postgres::{NoTls, Client};
use uuid::Uuid;
use warp::Filter;

#[derive(Clone)]
struct AppState {
    sessions: Arc<Mutex<HashMap<Uuid, Session>>>,
    config: Arc<Config>,
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

#[derive(Debug, Deserialize)]
struct Config {
    postgres_url: String,
    ws: Option<EndpointConfig>,
    wss: Option<WssConfig>,
}

#[derive(Debug, Deserialize, Clone)]
struct EndpointConfig {
    bind: String,
}

#[derive(Debug, Deserialize, Clone)]
struct WssConfig {
    bind: String,
    cert_path: String,
    key_path: String,
}

#[tokio::main]
async fn main() {
    let config_path = std::env::args().nth(1).unwrap_or_else(|| "config.yml".into());
    let file = File::open(&config_path).expect("unable to open config file");
    let config: Config = serde_yaml::from_reader(file).expect("invalid config");
    let config = Arc::new(config);

    let state = AppState {
        sessions: Arc::new(Mutex::new(HashMap::new())),
        config: config.clone(),
    };

    let state_filter = warp::any().map(move || state.clone());

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(state_filter)
        .map(|ws: warp::ws::Ws, state: AppState| {
            ws.on_upgrade(move |socket| client_connection(socket, state))
        });

    let mut servers = vec![];

    if let Some(ws_cfg) = config.ws.clone() {
        let route = ws_route.clone();
        let addr: SocketAddr = ws_cfg.bind.parse().expect("invalid ws bind address");
        println!("Listening ws on {}", addr);
        servers.push(tokio::spawn(warp::serve(route).run(addr)));
    }

    if let Some(wss_cfg) = config.wss.clone() {
        let route = ws_route;
        let addr: SocketAddr = wss_cfg.bind.parse().expect("invalid wss bind address");
        println!("Listening wss on {}", addr);
        servers.push(tokio::spawn(warp::serve(route).tls()
            .cert_path(wss_cfg.cert_path)
            .key_path(wss_cfg.key_path)
            .run(addr)));
    }

    futures::future::join_all(servers).await;
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
    let conn_str = format!("{} user={} password={}", state.config.postgres_url, user, password);
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
