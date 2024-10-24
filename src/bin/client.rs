use chipmunk::client::ChipmunkClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let c = ChipmunkClient::try_new("127.0.0.1:5000".to_string())?;
    println!("{:?}", c.insert("key", "value").await?);
    println!("{:?}", c.get("key").await?);
    println!("{:?}", c.delete("key").await?);
    println!("{:?}", c.delete("key").await?);
    Ok(())
}
