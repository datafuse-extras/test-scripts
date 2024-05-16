use anyhow::Result;
use databend_driver::Client;
mod util;
use util::ConnectionExt;

const SET_UP: &str = "./sql/setup.sql";
const MULTI_INSERT: &str = "./sql/multi_table_insert.sql";
const RUN: usize = 100;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("DATABEND_DSN").unwrap_or(
        "databend://root:@localhost:8000/default?sslmode=disable&enable_experimental_merge_into=1"
            .to_owned(),
    );
    let client = Client::new(dsn);
    let c1 = client.get_conn().await?;
    c1.exec_lines(SET_UP).await?;
    let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut join_handles = vec![];
    for i in 0..9 {
        let stop_flag = stop_flag.clone();
        let client = client.clone();
        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let c = client.get_conn().await.unwrap();
            loop {
                if stop_flag.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                c.exec(&format!("optimize table t{} compact segment;", i))
                    .await?;
                c.exec(&format!("optimize table t{} compact;", i)).await?;
                c.exec(&format!("optimize table t{} purge;", i)).await?;
                c.exec(&format!("alter table t{} recluster;", i)).await?;
            }
            Ok(())
        });
        join_handles.push(handle);
    }

    let mut success = 0;
    for i in 0..RUN {
        let start = std::time::Instant::now();
        let c = client.get_conn().await?;
        match c.exec_lines(MULTI_INSERT).await {
            Ok(_) => {
                success += 1;
            }
            Err(e) => {
                println!("multi table insert {} failed: {:?}", i, e);
            }
        }
        println!("multi table insert {} cost {:?}", i, start.elapsed());
    }

    stop_flag.store(true, std::sync::atomic::Ordering::Release);
    for handle in join_handles {
        handle.await??;
    }

    println!("success insertions / runs : {}/{}", success, RUN);
    // verify
    for i in 0..10 {
        let c = client.get_conn().await?;
        println!("verify {}", i);
        c.assert_query(
            &format!("SELECT count(*) FROM t{} WHERE c % 10 <> {};", i, i),
            vec![(0,)],
        )
        .await;
        c.assert_query(
            &format!("SELECT count(*) FROM t{};", i),
            vec![(success * 10000 / 10,)],
        )
        .await;
    }

    println!("---All tests passed!---");

    Ok(())
}
