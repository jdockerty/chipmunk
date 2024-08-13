use std::{net::TcpListener, sync::mpsc::Receiver};

use crate::lsm::{Lsm, MemtableConfig, WalConfig};

pub struct ChipmunkHandle {}

/// An instance of the chipmunk store.
///
/// This comprises of the underlying k-v store and server.
pub struct Chipmunk {
    store: Lsm,
    store_channel: Receiver<(Vec<u64>, Vec<u64>)>,
    server_addr: String,
}

pub struct ChipmunkConfig {
    pub addr: String,
    pub wal: WalConfig,
    pub memtable: MemtableConfig,
}

impl Chipmunk {
    pub fn new(config: ChipmunkConfig, recv: Receiver<(Vec<u64>, Vec<u64>)>) -> Self {
        Self {
            store: Lsm::new(config.wal, config.memtable),
            server_addr: config.addr,
            store_channel: recv,
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::lsm::{MemtableConfig, WalConfig};

    use super::{Chipmunk, ChipmunkConfig};

    #[test]
    fn chipmunk() {
        let t = TempDir::new("t").unwrap();
        let conf = ChipmunkConfig {
            wal: WalConfig::new(0, 1024, t.path().to_path_buf()),
            memtable: MemtableConfig::new(0, 1024),
            addr: "127.0.0.1:4000".to_string(),
        };
        let (_tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            Chipmunk::new(conf, rx);
        });
    }
}
