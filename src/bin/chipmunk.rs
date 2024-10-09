use chipmunk::{
    config::{ChipmunkConfig, MemtableConfig, WalConfig},
    server::Chipmunk,
};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
        .init();

    let config = ChipmunkConfig {
        wal: WalConfig {
            id: 0,
            max_size: 1024,
            log_directory: "./".into(),
            buffer_size: None,
        },
        memtable: MemtableConfig {
            id: 0,
            max_size: 1024,
        },
    };

    let c = Chipmunk::new(config);
    info!("Listening on http://127.0.0.1:5000");
    let app = chipmunk::server::new_app(c);
    let listener = TcpListener::bind("127.0.0.1:5000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
