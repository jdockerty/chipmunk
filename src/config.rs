use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub id: u64,
    pub max_size: u64,
    pub log_directory: PathBuf,
}

impl WalConfig {
    pub fn new(id: u64, max_size: u64, log_directory: PathBuf) -> Self {
        Self {
            id,
            max_size,
            log_directory,
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
    pub addr: String,
    pub wal: WalConfig,
    pub memtable: MemtableConfig,
}
