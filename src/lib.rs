use std::io;
use std::path::PathBuf;

use axum::http::StatusCode;

pub mod client;
pub mod config;
pub mod server;

mod lsm;
mod memtable;
mod wal;

#[derive(Debug, thiserror::Error)]
pub enum ChipmunkError {
    #[error("an invalid segment file")]
    InvalidSegment { name: String, source: io::Error },

    #[error("unable to fsync the current segment")]
    SegmentFsync(io::Error),

    #[error("unable to open wal file")]
    SegmentOpen(io::Error),

    #[error("could not append to the WAL segment")]
    WalAppend(io::Error),

    #[error("unable to open WAL directory '{path}': {source} ")]
    WalDirectoryOpen { source: io::Error, path: PathBuf },

    #[error("unable to open directory to restore: {0}")]
    WalRestoreDirectory(io::Error),
}

impl ChipmunkError {
    fn as_status_code(&self) -> StatusCode {
        // NOTE: new errors may not always be surfaced as an internal server
        // error so we can allow this for now.
        #[allow(clippy::match_single_binding)]
        match self {
            // The internal error should be masked. We do not want to leak
            // errors relating to underlying k-v operations over the outward
            // facing HTTP API.
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
