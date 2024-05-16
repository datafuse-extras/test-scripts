use std::vec;

use anyhow::Result;
use databend_driver::Client;
use util::ConnectionExt;
mod util;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("DATABEND_DSN").unwrap_or(
        "databend://root:@localhost:8000/default?sslmode=disable&enable_experimental_merge_into=1"
            .to_owned(),
    );

    let client = Client::new(dsn);
    let c1 = client.get_conn().await.unwrap();
    let c2 = client.get_conn().await.unwrap();

    let select_t = "SELECT * FROM t ORDER BY c;";

    c1.exec("CREATE OR REPLACE TABLE t(c int);").await?;

    // c1 commit failed due to c2 has modified the data
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
    c2.assert_query(select_t, vec![(2,)]).await;

    // rollback
    c1.begin().await?;
    c1.exec("INSERT INTO t VALUES(1);").await?;
    let result = c1.exec("qwerty").await;
    assert!(result.is_err());
    c1.commit().await?;
    c1.assert_query(select_t, vec![(2,)]).await;
    c2.assert_query(select_t, vec![(2,)]).await;

    // rollback
    c1.exec("drop table if exists t1;").await?;
    c1.begin().await?;
    c1.exec("INSERT INTO t VALUES(1);").await?;
    let result = c1.exec("select * from t1").await;
    assert!(result.is_err());
    c1.commit().await?;
    c1.assert_query(select_t, vec![(2,)]).await;
    c2.assert_query(select_t, vec![(2,)]).await;

    //stream
    c1.exec("create or replace table base(c int);").await?;

    c1.exec("CREATE or replace STREAM s ON TABLE base APPEND_ONLY=true;")
        .await?;

    c1.begin().await?;
    c1.exec("INSERT INTO base VALUES(1);").await?;
    // First time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;

    c2.begin().await?;
    c2.exec("INSERT INTO base VALUES(2);").await?;
    c2.commit().await?;
    // Second time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;

    c1.exec("Insert into base values(3);").await?;
    // Third time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;
    let result = c1.commit().await;
    assert!(result.is_err());

    // no conflict, both commit success
    c1.assert_query(select_t, vec![(2,)]).await;
    c2.assert_query(select_t, vec![(2,)]).await;
    c1.exec("CREATE OR REPLACE TABLE t1(c int);").await?;
    let select_t1 = "SELECT * FROM t1 ORDER BY c;";

    c1.begin().await?;
    c1.exec("INSERT INTO t VALUES(1);").await?;
    c1.assert_query(select_t, vec![(1,), (2,)]).await;
    c2.assert_query(select_t, vec![(2,)]).await;

    c2.begin().await?;
    c2.exec("INSERT INTO t1 VALUES(3);").await?;
    c1.assert_query::<(i32,)>(select_t1, vec![]).await;
    c2.assert_query(select_t1, vec![(3,)]).await;

    c2.commit().await?;
    c1.commit().await?;
    c1.assert_query(select_t, vec![(1,), (2,)]).await;
    c2.assert_query(select_t, vec![(1,), (2,)]).await;
    c1.assert_query(select_t1, vec![(3,)]).await;
    c2.assert_query(select_t1, vec![(3,)]).await;
    println!("All tests passed!");
    Ok(())
}
