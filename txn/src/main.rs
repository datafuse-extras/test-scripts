use std::vec;

use anyhow::Result;
use databend_driver::Client;
use util::ConnectionExt;
mod util;

#[tokio::main]
async fn main() -> Result<()> {
    //export DATABEND_DSN="databend://root:@localhost:8000/?sslmode=disable"
    let dsn = std::env::var("DATABEND_DSN").unwrap();

    let client = Client::new(dsn);
    let c1 = client.get_conn().await.unwrap();
    let c2 = client.get_conn().await.unwrap();

    let select_t = "SELECT * FROM t ORDER BY c;";

    c1.exec("CREATE OR REPLACE TABLE t(c int);").await?;

    c1.begin().await?;
    c1.exec("INSERT INTO t VALUES(1);").await?;
    c1.assert_query(select_t, vec![(1,)]).await;
    c2.assert_query::<(i32,)>(select_t, vec![]).await;

    c2.begin().await?;
    c2.exec("INSERT INTO t VALUES(2);").await?;
    c1.assert_query(select_t, vec![(1,)]).await;
    c2.assert_query(select_t, vec![(2,)]).await;

    c2.commit().await?;
    c1.assert_query(select_t, vec![(1,)]).await;
    c2.assert_query(select_t, vec![(2,)]).await;

    let result = c1.commit().await;
    assert!(result.is_err());
    c1.assert_query(select_t, vec![(2,)]).await;
    // c2.assert_query(select_t, vec![(2,)]).await;
    Ok(())
}
