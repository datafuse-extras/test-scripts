use anyhow::Result;
use databend_driver::Client;
mod util;
use util::ConnectionExt;

const SET_UP: &str = "set_up.sql";
const MERGE: &str = "merge.sql";
const TXN_MERGE: &str = "txn_merge.sql";
const TASK_TXN_MERGE: &str = "task_txn_merge.sql";
#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("BENDSQL_DSN").unwrap();
    let client = Client::new(dsn);
    {
        set_up(&client).await?;
        let c = client.get_conn().await?;
        c.exec_lines(MERGE).await?;
        c.assert_query("select count(*) from json_table;", vec![(10000,)])
            .await;
        c.assert_query("select count(*) from json_table_stream;", vec![(10000,)])
            .await;
        println!("merge result:");
        print_result(&client).await?;
    }
    {
        set_up(&client).await?;
        let c = client.get_conn().await?;
        c.exec_lines(TXN_MERGE).await?;
        c.assert_query("select count(*) from json_table;", vec![(10000,)])
            .await;
        c.assert_query("select count(*) from json_table_stream;", vec![(0,)])
            .await;
        println!("merge + txn result:");
        print_result(&client).await?;
    }
    {
        set_up(&client).await?;
        let c = client.get_conn().await?;
        let sql = std::fs::read_to_string(TASK_TXN_MERGE)?;
        c.exec("drop task if exists merge_task;").await?;
        c.exec(&sql).await?;
        c.exec("execute task merge_task").await?;
        c.assert_query("select count(*) from json_table;", vec![(10000,)])
            .await;
        c.assert_query("select count(*) from json_table_stream;", vec![(0,)])
            .await;
        println!("merge + txn + task result:");
        print_result(&client).await?;
    }

    Ok(())
}

async fn set_up(client: &Client) -> Result<()> {
    let c = client.get_conn().await?;
    c.exec_lines(SET_UP).await?;
    c.assert_query("select count(*) from json_table;", vec![(10000,)])
        .await;
    c.assert_query("select count(*) from json_table_stream;", vec![(10000,)])
        .await;
    Ok(())
}

async fn print_result(client: &Client) -> Result<()> {
    let c = client.get_conn().await?;
    let json_table_flag_1: Vec<(u32,)> = c
        .exec_query("select count(*) from json_table_flag_1;")
        .await?;
    let json_table_flag_2: Vec<(u32,)> = c
        .exec_query("select count(*) from json_table_flag_2;")
        .await?;
    let json_table_flag_3: Vec<(u32,)> = c
        .exec_query("select count(*) from json_table_flag_3;")
        .await?;
    let json_table_flag_4: Vec<(u32,)> = c
        .exec_query("select count(*) from json_table_flag_4;")
        .await?;
    println!("json_table_flag_1: {:?}, json_table_flag_2: {:?}, json_table_flag_3: {:?}, json_table_flag_4: {:?}", json_table_flag_1, json_table_flag_2, json_table_flag_3, json_table_flag_4);
    Ok(())
}
