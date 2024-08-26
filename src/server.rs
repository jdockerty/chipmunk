use std::{
    io::Read,
    net::TcpListener,
    sync::mpsc::{channel, Receiver, Sender},
};

use crate::config::ChipmunkConfig;
use crate::lsm::Lsm;

/// The `ChipmunkHandle` takes care of passing messages (key-value pairs) from
/// an incoming stream to the [`Chipmunk`] store.
///
/// This utilises the actor pattern. This struct is the handle.
pub struct ChipmunkHandle {
    server_addr: String,
    store_tx: Sender<(Vec<u8>, Vec<u8>)>,
}

impl ChipmunkHandle {
    pub fn new(addr: String, config: ChipmunkConfig) -> Self {
        let (tx, rx) = channel();
        let c = Chipmunk::new(config, rx);

        std::thread::spawn(move || {
            while let Ok((key, value)) = c.store_rx.recv() {
                eprintln!("{key:?}={value:?}");
            }
        });

        Self {
            server_addr: addr,
            store_tx: tx,
        }
    }

    /// Starts the handle, which corresponds to handling incoming requests via
    /// TCP. These are automatically handed off to the [`Chipmunk`] store
    /// through actor pattern.
    pub fn start(&self) {
        let listener = TcpListener::bind(&self.server_addr).unwrap();
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).unwrap();
            let buf = buf.trim().split(|x| x == '=').collect::<Vec<_>>();

            match (buf.first(), buf.get(1)) {
                (Some(key), Some(value)) => {
                    eprintln!("{key}={value}");
                    self.store_tx
                        .send((key.as_bytes().to_vec(), value.as_bytes().to_vec()))
                        .unwrap();
                }
                (None, _) | (Some(_), None) => {
                    eprintln!("Did not get key=value format");
                }
            }
        }
    }
}

/// An instance of the chipmunk store.
///
/// This comprises of the underlying k-v store and server. This utilises the
/// actor pattern for communication. This is the task portion.
pub struct Chipmunk {
    #[allow(dead_code)]
    store: Lsm,
    store_rx: Receiver<(Vec<u8>, Vec<u8>)>,
}

impl Chipmunk {
    pub fn new(config: ChipmunkConfig, rx: Receiver<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            store: Lsm::new(config.wal, config.memtable),
            store_rx: rx,
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::config::{ChipmunkConfig, MemtableConfig, WalConfig};

    use super::ChipmunkHandle;

    #[test]
    fn chipmunk() {
        let t = TempDir::new("t").unwrap();
        let conf = ChipmunkConfig {
            wal: WalConfig::new(0, 1024, t.path().to_path_buf()),
            memtable: MemtableConfig::new(0, 1024),
        };

        std::thread::spawn(move || {
            let handle = ChipmunkHandle::new("127.0.0.1:55555".to_string(), conf);
            handle.start();
        });
    }
}
