use std::vec;

use anyhow::Result;
use databend_driver::Client;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("DATABEND_DSN").unwrap();
    let file_name = "task.sql";
    let sql = std::fs::read_to_string(file_name)?;
    let client = Client::new(dsn);
    let c = client.get_conn().await?;
    c.exec(&sql).await?;
    Ok(())
}
