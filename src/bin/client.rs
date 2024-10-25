use chipmunk::client::ChipmunkClient;
use clap::{Parser, Subcommand};

#[derive(Debug, Clone, Parser)]
struct Cli {
    /// Host address of the remote chipmunk store.
    #[arg(long, default_value = "127.0.0.1:5000")]
    host: String,

    #[clap(subcommand)]
    commands: Commands,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    /// Get a value from the store, addressed by key.
    Get { key: String },
    /// Insert a key-value pair to the store.
    Insert { key: String, value: String },
    /// Delete a pre-existing key-value pair from the store, addressed by key.
    Delete { key: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let cli = Cli::parse();

    let client = ChipmunkClient::try_new(cli.host)?;

    match cli.commands {
        Commands::Get { key } => {
            match client.get(&key).await? {
                Some(value) => println!("{value}"),
                None => println!("'{key}' does not exist"),
            };
        }
        Commands::Insert { key, value } => client.insert(&key, &value).await?,
        Commands::Delete { key } => client.delete(&key).await?,
    }
    Ok(())
}
