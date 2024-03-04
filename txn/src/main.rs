use std::vec;

use anyhow::Result;
use databend_driver::new_connection;
use util::ConnectionExt;
mod util;
#[tokio::main]
async fn main() -> Result<()> {
    //export DATABEND_DSN="databend://root:@localhost:8000/?sslmode=disable"
    let dsn = std::env::var("DATABEND_DSN").unwrap();

    let c1 = new_connection(&dsn)?;
    let c2 = new_connection(&dsn)?;

    let select_t = "SELECT * FROM t ORDER BY c;";

    c1.exec("CREATE OR REPLACE TABLE t(c int);").await?;

    c1.begin().await?;
    c1.exec("INSERT INTO t VALUES(2);").await?;
    c1.assert_query(select_t, vec![(2,)]).await;

    c2.assert_query::<(i32,)>(select_t, vec![]).await;
    Ok(())
}
