use std::io;
use std::path::PathBuf;

pub mod config;
pub mod server;

mod lsm;
mod memtable;
mod wal;

#[derive(Debug, thiserror::Error)]
pub enum ChipmunkError {
    #[error("An invalid segment file")]
    InvalidSegment { name: String, source: io::Error },

    #[error("Unable to fsync the current segment")]
    SegmentFsync(io::Error),

    #[error("Unable to open wal file")]
    SegmentOpen(io::Error),

    #[error("Could not append to the WAL segment")]
    WalAppend(io::Error),

    #[error("Unable to open WAL directory '{path}': {source} ")]
    WalDirectoryOpen { source: io::Error, path: PathBuf },
}
