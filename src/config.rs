use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub id: u64,
    pub max_size: u64,
    pub log_directory: PathBuf,
    pub buffer_size: Option<usize>,
}

impl WalConfig {
    pub fn new(id: u64, max_size: u64, log_directory: PathBuf, buffer_size: Option<usize>) -> Self {
        Self {
            id,
            max_size,
            log_directory,
            buffer_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemtableConfig {
    pub id: u64,
    pub max_size: u64,
}

impl MemtableConfig {
    pub fn new(id: u64, max_size: u64) -> Self {
        Self { id, max_size }
    }
}

pub struct ChipmunkConfig {
    pub wal: WalConfig,
    pub memtable: MemtableConfig,
}
