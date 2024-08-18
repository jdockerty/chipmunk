use std::{
    io::{BufRead, Read},
    net::TcpListener,
    sync::mpsc::{self, channel, Receiver, Sender},
};

use crate::config::ChipmunkConfig;
use crate::lsm::Lsm;

/// An instance of the chipmunk store.
///
/// This comprises of the underlying k-v store and server.
pub struct Chipmunk {
    store: Lsm,
    server_addr: String,

    store_tx: Sender<(Vec<u8>, Vec<u8>)>,
    store_rx: Receiver<(Vec<u8>, Vec<u8>)>,
}

impl Chipmunk {
    pub fn run(&mut self) {
        let listener = TcpListener::bind(self.server_addr.clone()).unwrap();

        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).unwrap();
            let buf = buf.trim().split(|x| x == '=').collect::<Vec<_>>();
            let key = buf[0];
            let value = buf[1];
            self.store_tx
                .send((key.as_bytes().to_vec(), value.as_bytes().to_vec()))
                .unwrap();
        }
    }
}

impl Chipmunk {
    pub fn new(config: ChipmunkConfig) -> Self {
        let (tx, rx) = channel();
        Self {
            store: Lsm::new(config.wal, config.memtable),
            server_addr: config.addr,
            store_tx: tx,
            store_rx: rx,
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::config::{ChipmunkConfig, MemtableConfig, WalConfig};

    use super::Chipmunk;

    #[test]
    fn chipmunk() {
        let t = TempDir::new("t").unwrap();
        let conf = ChipmunkConfig {
            wal: WalConfig::new(0, 1024, t.path().to_path_buf()),
            memtable: MemtableConfig::new(0, 1024),
            addr: "127.0.0.1:4000".to_string(),
        };

        std::thread::spawn(move || {
            Chipmunk::new(conf);
        });
    }
}
