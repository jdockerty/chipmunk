use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::Router;
use std::sync::Arc;

use crate::config::ChipmunkConfig;
use crate::lsm::Lsm;

pub fn new_app(store: Chipmunk) -> Router {
    let store = Arc::new(store);
    Router::new()
        .route("/health", get(|| async move { "OK" }))
        .route("/api/v1/:key", get(get_key_handler))
        .route("/api/v1", post(add_kv_handler))
        .route("/api/v1/:key", delete(delete_key_handler))
        .with_state(store)
}

async fn get_key_handler(
    Path(key): Path<String>,
    State(state): State<Arc<Chipmunk>>,
) -> impl IntoResponse {
    match state.store().get(key.into_bytes()) {
        Some(value) => (value).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_key_handler(
    Path(key): Path<String>,
    State(state): State<Arc<Chipmunk>>,
) -> impl IntoResponse {
    state.store().delete(key.into_bytes());
    StatusCode::NO_CONTENT
}

async fn add_kv_handler(
    State(state): State<Arc<Chipmunk>>,
    req: String,
) -> impl axum::response::IntoResponse {
    match req.split_once("=") {
        Some((key, value)) => {
            state.store().insert(key.into(), value.into());
            StatusCode::NO_CONTENT.into_response()
        }
        None => (StatusCode::BAD_REQUEST, "Must provide key=value format").into_response(),
    }
}

/// An instance of the chipmunk store.
///
/// This comprises of the underlying k-v store and server. This utilises the
/// actor pattern for communication. This is the task portion.
pub struct Chipmunk {
    store: Arc<Lsm>,
}

impl Chipmunk {
    pub fn new(config: ChipmunkConfig) -> Self {
        Self {
            store: Arc::new(Lsm::new(config.wal, config.memtable)),
        }
    }

    pub fn store(&self) -> &Lsm {
        &self.store
    }
}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;

    use tokio::net::TcpListener;

    use tempdir::TempDir;

    use super::*;
    use crate::config::{ChipmunkConfig, MemtableConfig, WalConfig};

    fn get_base_uri(addr: SocketAddr) -> String {
        format!("http://{addr}/api/v1")
    }

    async fn setup_server(conf: ChipmunkConfig) -> SocketAddr {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let store = Chipmunk::new(conf);
            let app = new_app(store);
            axum::serve(socket, app).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn chipmunk_invalid_add() {
        let dir = TempDir::new("invalid_post").unwrap();
        let conf = ChipmunkConfig {
            wal: WalConfig::new(0, 1024, dir.path().to_path_buf()),
            memtable: MemtableConfig::new(0, 1024),
        };
        let addr = setup_server(conf).await;
        let client = reqwest::Client::new();
        let base = get_base_uri(addr);

        let response = client.post(&base).body("key1,value1").send().await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.text().await.unwrap(),
            "Must provide key=value format"
        );
    }

    #[tokio::test]
    async fn chipmunk_crud() {
        let dir = TempDir::new("write_kv").unwrap();
        let conf = ChipmunkConfig {
            wal: WalConfig::new(0, 1024, dir.path().to_path_buf()),
            memtable: MemtableConfig::new(0, 1024),
        };
        let addr = setup_server(conf).await;
        let client = reqwest::Client::new();
        let base = get_base_uri(addr);

        client.post(&base).body("key1=value1").send().await.unwrap();

        let got = client
            .get(format!("{base}/key1"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(got, "value1");

        let r = client.delete(format!("{base}/key1")).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        let got = client.get(format!("{base}/key1")).send().await.unwrap();
        assert_eq!(got.status(), StatusCode::NOT_FOUND);
    }
}
