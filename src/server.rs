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
    println!("GET {key}");
    match state.store().get(key.into_bytes()) {
        Some(value) => (value).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_key_handler(Path(key): Path<String>, State(_state): State<Arc<Chipmunk>>) {
    println!("DELETE {key}");
    unimplemented!()
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
    #[allow(dead_code)]
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
    use tempdir::TempDir;

    use crate::config::{ChipmunkConfig, MemtableConfig, WalConfig};

    #[tokio::test]
    async fn chipmunk() {
        let t = TempDir::new("t").unwrap();
        let _conf = ChipmunkConfig {
            wal: WalConfig::new(0, 1024, t.path().to_path_buf()),
            memtable: MemtableConfig::new(0, 1024),
        };

        tokio::spawn(async move {});
    }
}
