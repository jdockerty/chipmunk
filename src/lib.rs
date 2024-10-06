use std::io;

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

    #[error("Could not append to the WAL segment")]
    WalAppend(io::Error)
}
