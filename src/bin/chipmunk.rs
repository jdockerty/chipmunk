use chipmunk::{
    config::{ChipmunkConfig, MemtableConfig, WalConfig},
    server::Chipmunk,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
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

    let c = Chipmunk::new(config);
    eprintln!("Listening on http://127.0.0.1:5000");
    let app = chipmunk::server::new_app(c);
    let listener = TcpListener::bind("127.0.0.1:5000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
