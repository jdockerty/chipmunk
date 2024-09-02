use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::Router;
use std::sync::mpsc::{channel, Receiver, Sender};
use tokio::net::TcpListener;

use crate::config::ChipmunkConfig;
use crate::lsm::Lsm;

#[derive(Debug)]
pub enum Msg {
    Get { key: Vec<u8> },
    Insert { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub fn new_app(tx: Sender<Msg>) -> Router {
    Router::new()
        .route("/health", get(|| async move { "OK" }))
        .route("/api/v1/:key", get(get_key_handler))
        .route("/api/v1", post(add_kv_handler))
        .route("/api/v1/:key", delete(delete_key_handler))
        .with_state(tx)
}

async fn get_key_handler(Path(key): Path<String>, State(state): State<Sender<Msg>>) {
    println!("GET {key}");
    state
        .send(Msg::Get {
            key: key.into_bytes(),
        })
        .unwrap();
}

async fn delete_key_handler(Path(key): Path<String>, State(state): State<Sender<Msg>>) {
    println!("DELETE {key}");
    state
        .send(Msg::Delete {
            key: key.into_bytes(),
        })
        .unwrap();
}

async fn add_kv_handler(
    State(state): State<Sender<Msg>>,
    req: String,
) -> impl axum::response::IntoResponse {
    match req.split_once("=") {
        Some((key, value)) => {
            state
                .send(Msg::Insert {
                    key: key.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                })
                .unwrap();
            StatusCode::NO_CONTENT.into_response()
        }
        None => (
            StatusCode::BAD_REQUEST,
            "Must provide key=value format",
        )
            .into_response(),
    }
}

/// The `ChipmunkHandle` takes care of passing messages (key-value pairs) from
/// an incoming stream to the [`Chipmunk`] store.
///
/// This utilises the actor pattern. This struct is the handle.
#[derive(Clone)]
pub struct ChipmunkHandle {
    server_addr: String,
    store_tx: Sender<Msg>,
}

impl ChipmunkHandle {
    pub fn new(addr: String, config: ChipmunkConfig) -> Self {
        let (tx, rx) = channel();
        let c = Chipmunk::new(config, rx);

        std::thread::spawn(move || {
            while let Ok(msg) = c.store_rx.recv() {
                eprintln!("Got msg {msg:?}");
            }
        });

        Self {
            server_addr: addr,
            store_tx: tx,
        }
    }

    pub fn sender(&self) -> Sender<Msg> {
        self.store_tx.clone()
    }

    /// Starts the handle, which corresponds to handling incoming requests via
    /// TCP. These are automatically handed off to the [`Chipmunk`] store
    /// through actor pattern.
    pub async fn start(&self, app: Router) {
        let listener = TcpListener::bind(&self.server_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap()
    }
}

/// An instance of the chipmunk store.
///
/// This comprises of the underlying k-v store and server. This utilises the
/// actor pattern for communication. This is the task portion.
pub struct Chipmunk {
    #[allow(dead_code)]
    store: Lsm,
    store_rx: Receiver<Msg>,
}

impl Chipmunk {
    pub fn new(config: ChipmunkConfig, rx: Receiver<Msg>) -> Self {
        Self {
            store: Lsm::new(config.wal, config.memtable),
            store_rx: rx,
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::config::{ChipmunkConfig, MemtableConfig, WalConfig};

    use super::{new_app, ChipmunkHandle};

    #[tokio::test]
    async fn chipmunk() {
        let t = TempDir::new("t").unwrap();
        let conf = ChipmunkConfig {
            wal: WalConfig::new(0, 1024, t.path().to_path_buf()),
            memtable: MemtableConfig::new(0, 1024),
        };

        tokio::spawn(async move {
            let handle = ChipmunkHandle::new("127.0.0.1:55555".to_string(), conf);
            let app = new_app(handle.sender());
            handle.start(app).await;
        });
    }
}
