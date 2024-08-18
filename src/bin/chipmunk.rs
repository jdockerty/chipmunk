use std::sync::mpsc::channel;

use chipmunk::{
    config::{ChipmunkConfig, MemtableConfig, WalConfig},
    server::Chipmunk,
};

fn main() {
    let config = ChipmunkConfig {
        addr: "127.0.0.1:5000".to_string(),
        wal: WalConfig {
            id: 0,
            max_size: 1024,
            log_directory: "./".into(),
        },
        memtable: MemtableConfig {
            id: 0,
            max_size: 1024,
        },
    };
    let mut h = Chipmunk::new(config);
    eprintln!("Listening on tcp://127.0.0.1:5000");
    h.run();
}
