use chipmunk::{
    config::{ChipmunkConfig, MemtableConfig, WalConfig},
    server::ChipmunkHandle,
};

fn main() {
    let config = ChipmunkConfig {
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

    let handle = ChipmunkHandle::new("127.0.0.1:5000".to_string(), config);
    eprintln!("Listening on tcp://127.0.0.1:5000");
    handle.start();
}
