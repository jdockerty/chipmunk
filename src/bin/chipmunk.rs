use chipmunk::{
    config::{ChipmunkConfig, MemtableConfig, WalConfig},
    server::Chipmunk,
};
use clap::Parser;
use clap_verbosity::InfoLevel;
use tokio::net::TcpListener;
use tracing::info;
use tracing_log::AsTrace;

use std::path::PathBuf;

#[derive(Debug, Parser)]
struct Cli {
    /// Address to bind to for listening on incoming connections.
    #[arg(long, default_value = "127.0.0.1:5000")]
    bind_address: String,

    #[command(flatten)]
    log_level: clap_verbosity::Verbosity<InfoLevel>,

    /// Directory that WAL segments should be written to.
    #[arg(long, default_value = "./")]
    wal_directory: PathBuf,

    /// Maximium size, in bytes, of the WAL before rotation should occur.
    ///
    /// Default to 8 MiB.
    #[arg(long, default_value = "8388608")] //
    wal_max_size_bytes: u64,

    /// Size, in bytes, of the internal WAL buffer.
    #[arg(long)]
    wal_buffer_size_bytes: Option<usize>,

    /// Maximium size, in bytes, of the memtable before it is flushed to disk.
    ///
    /// Defaults to 8 MiB.
    #[arg(long, default_value = "8388608")]
    memtable_max_size_bytes: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_max_level(cli.log_level.log_level_filter().as_trace())
        .init();

    let config = ChipmunkConfig {
        wal: WalConfig {
            id: 0,
            max_size: cli.wal_max_size_bytes,
            log_directory: cli.wal_directory,
            buffer_size: cli.wal_buffer_size_bytes,
        },
        memtable: MemtableConfig {
            id: 0,
            max_size: cli.memtable_max_size_bytes,
        },
    };

    let c = Chipmunk::new(config);
    c.restore().await?;
    info!("Listening on http://{}", cli.bind_address);
    let app = chipmunk::server::new_app(c);
    let listener = TcpListener::bind(cli.bind_address).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
