use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::Router;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::config::ChipmunkConfig;
use crate::lsm::Lsm;
use crate::ChipmunkError;

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
    match state.store.read().await.get(key.into_bytes()) {
        Some(value) => (value).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_key_handler(
    Path(key): Path<String>,
    State(state): State<Arc<Chipmunk>>,
) -> impl IntoResponse {
    match state.store.write().await.delete(key.as_bytes().to_vec()) {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!("Cannot delete '{key}': {e}");
            e.as_status_code()
        }
    }
}

async fn add_kv_handler(State(state): State<Arc<Chipmunk>>, req: String) -> impl IntoResponse {
    match req.split_once("=") {
        Some((key, value)) => match state.store.write().await.insert(key.into(), value.into()) {
            Ok(_) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => {
                warn!("Cannot insert '{key}': {e}");
                let err = format!("Cannot insert '{key}'");
                (e.as_status_code(), err).into_response()
            }
        },
        None => (StatusCode::BAD_REQUEST, "Must provide key=value format").into_response(),
    }
}

/// An instance of the [`Chipmunk`] store.
///
/// This comprises of the underlying k-v store and server. This utilises the
/// actor pattern for communication. This is the task portion.
#[derive(Clone)]
pub struct Chipmunk {
    store: Arc<RwLock<Lsm>>,
}

impl Chipmunk {
    pub fn new(config: ChipmunkConfig) -> Self {
        Self {
            store: Arc::new(RwLock::new(Lsm::new(config.wal, config.memtable))),
        }
    }

    /// Attempt to perform a restore of the store.
    pub async fn restore(&self) -> Result<(), ChipmunkError> {
        if self.should_restore().await? {
            self.store.write().await.restore()?;
        }
        Ok(())
    }

    /// Determine whether a restore is possible.
    ///
    /// # Notes
    /// This is a simple check as to whether there are any other files present
    /// within the log directory when the server is started.
    async fn should_restore(&self) -> Result<bool, ChipmunkError> {
        // TODO: this method of checking for restore will do 2 instances of
        // a `read_dir`, we should be able to avoid this.
        let files = std::fs::read_dir(self.store.read().await.working_directory())
            .map_err(ChipmunkError::WalRestoreDirectory)?;
        let files = files.filter(|d| d.is_ok()).map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name();
            if name.to_string_lossy().contains("wal") && entry.metadata().unwrap().len() > 0 {
                Some(entry)
            } else {
                None
            }
        }).collect::<Vec<_>>();

        if files.is_empty() {
            debug!("Restore will not be attempted");
            Ok(false)
        } else {

            debug!(num_files=files.len(), "Restore will be attempted");
            Ok(true)
        }
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
            wal: WalConfig::new(0, 1024, dir.path().to_path_buf(), None),
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
            wal: WalConfig::new(0, 1024, dir.path().to_path_buf(), None),
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
