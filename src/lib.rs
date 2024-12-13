use std::io;
use std::path::PathBuf;

use axum::http::StatusCode;

pub mod client;
pub mod config;
pub mod server;

pub(crate) mod chipmunk {
    pub(crate) mod wal_proto {
        include!(concat!(env!("OUT_DIR"), "/chipmunk.wal_proto.rs"));
    }
}

mod lsm;
mod memtable;
mod wal;

#[derive(Debug, thiserror::Error)]
pub enum ChipmunkError {
    #[error("unable to fsync the current wal segment: {0}")]
    SegmentFsync(io::Error),

    #[error("unable to open wal segment file: {0}")]
    SegmentOpen(io::Error),

    #[error("unable to delete closed segment: {0}")]
    SegmentDelete(io::Error),

    #[error("could not append to the WAL segment: {0}")]
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
