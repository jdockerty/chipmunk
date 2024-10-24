use std::net::SocketAddr;

use reqwest::StatusCode;

/// Errors that originate from interacting with a remote chipmunk store.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("unable to get key '{key_name}': {source}")]
    GetOp {
        key_name: String,
        source: reqwest::Error,
    },

    #[error("unable to insert key '{key_name}': {source}")]
    InsertOp {
        key_name: String,
        source: reqwest::Error,
    },

    #[error("unable to delete key '{key_name}': {source}")]
    DeleteOp {
        key_name: String,
        source: reqwest::Error,
    },

    #[error("unable to parse given host '{host}': {source}")]
    InvalidHost {
        host: String,
        source: std::net::AddrParseError,
    },
}

/// Interact with a remote chipmunk store.
pub struct ChipmunkClient {
    host: SocketAddr,
}

impl ChipmunkClient {
    /// Attempt to create a new [`ChipmunkClient`].
    pub fn try_new(host: String) -> Result<Self, ClientError> {
        Ok(Self {
            host: host
                .parse()
                .map_err(|e| ClientError::InvalidHost { host, source: e })?,
        })
    }

    /// Check the remote server is available to accept connections, returning
    /// [`Some`] if available and [`None`] otherwise.
    pub async fn ping(&self) -> Option<()> {
        tokio::net::TcpStream::connect(self.host)
            .await
            .ok()
            .map(|_| ())
    }

    /// Get a value from the remote store, addressed by its key.
    pub async fn get(&self, _key: String) {
        todo!()
    }

    /// Insert a new key-value pair.
    pub async fn insert(&self, _key: String, _value: Vec<u8>) {
        todo!()
    }

    /// Delete a key from the remote store.
    pub async fn delete(&self, _key: String) {
        todo!()
    }
}
