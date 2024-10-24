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
    client: reqwest::Client,
}

impl ChipmunkClient {
    /// Attempt to create a new [`ChipmunkClient`].
    pub fn try_new(host: String) -> Result<Self, ClientError> {
        Ok(Self {
            host: host
                .parse()
                .map_err(|e| ClientError::InvalidHost { host, source: e })?,
            client: reqwest::Client::new(),
        })
    }

    /// Check the remote server is available to accept connections, returning
    /// [`Some`] if available and [`None`] otherwise.
    pub async fn ping(&self) -> Option<()> {
        self.client
            .get(format!("http://{}/health", self.host))
            .send()
            .await
            .ok()
            .map(|_| ())
    }

    /// Get a value from the remote store, addressed by its key.
    pub async fn get(&self, key: &str) -> Result<Option<String>, ClientError> {
        let resp = self
            .client
            .get(format!("http://{}/api/v1/{}", self.host, key))
            .send()
            .await
            .map_err(|e| ClientError::GetOp {
                key_name: key.to_string(),
                source: e,
            })?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let body = resp.bytes().await.map_err(|e| ClientError::GetOp {
            key_name: key.to_string(),
            source: e,
        })?;
        Ok(Some(String::from_utf8_lossy(&body).to_string()))
    }

    /// Insert a new key-value pair.
    pub async fn insert(&self, key: &str, value: &str) -> Result<(), ClientError> {
        let req = self
            .client
            .post(format!("http://{}/api/v1", self.host))
            .body(format!("{key}={value}"));

        let resp = req.send().await.map_err(|e| ClientError::InsertOp {
            key_name: key.to_string(),
            source: e,
        })?;

        match resp.error_for_status() {
            Ok(_) => Ok(()),
            Err(e) => Err(ClientError::InsertOp {
                key_name: key.to_string(),
                source: e,
            }),
        }
    }

    /// Delete a key from the remote store.
    pub async fn delete(&self, key: &str) -> Result<(), ClientError> {
        self.client
            .delete(format!("http://{}/api/v1/{}", self.host, key))
            .send()
            .await
            .map(|_| Ok(()))
            .map_err(|e| ClientError::DeleteOp {
                key_name: key.to_string(),
                source: e,
            })?
    }
}
