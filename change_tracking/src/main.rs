use std::fs::read_to_string;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use databend_driver::{Client, Connection};
use env_logger::Env;
use log::info;
use tokio::task::JoinHandle;

/// Change Tracking Testing Script
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// number of derived streams
    #[arg(long, default_value_t = 5)]
    num_derived_streams: u32,

    /// degree of concurrency that a stream being consumed
    #[arg(long, default_value_t = 3)]
    stream_consumption_concurrency: u32,

    /// times that a stream should be consumed
    #[arg(long, default_value_t = 10)]
    times_consumption_per_stream: u32,

    /// show stream consumption errors
    #[arg(long, default_value_t = false)]
    show_stream_consumption_errors: bool,
}

struct Driver {
    args: Args,
    stop_flag: Arc<AtomicBool>,
    dsn: String,
}

impl Driver {
    fn new(args: Args) -> Self {
        Self {
            args,
            stop_flag: Arc::new(AtomicBool::new(false)),
            dsn: "databend://root:@localhost:8000/default?sslmode=disable".to_string(),
        }
    }

    async fn wait_stream_consuming(&self, handles: Vec<JoinHandle<Result<u32>>>) -> Result<u32> {
        let mut success = 0;
        for join_handle in handles {
            if let Ok(Ok(v)) = join_handle.await {
                success += v
            }
        }
        Ok(success)
    }

    async fn new_connection_with_test_db(&self) -> Result<Box<dyn Connection>> {
        let conn = self.new_connection().await?;
        // set current database to 'test_stream'
        conn.exec("use test_stream").await?;
        Ok(conn)
    }

    async fn new_connection(&self) -> Result<Box<dyn Connection>> {
        let client = Client::new(self.dsn.clone());
        let conn = client.get_conn().await?;
        Ok(conn)
    }

    async fn setup(&self) -> Result<()> {
        info!("=====running setup script====");

        let conn = self.new_connection().await?;
        let setup_script = read_to_string("tests/sql/setup.sql")?;

        let db_set_sqls = vec![
            "drop database if exists test_stream",
            "create database test_stream",
            "use test_stream",
        ];

        let setup_lines = setup_script.split(';');

        let sqls = db_set_sqls.into_iter().chain(setup_lines);

        for sql in sqls {
            let sql = sql.trim();
            info!("line: {}", sql);
            if !sql.is_empty() {
                info!("executing sql: {}", sql);
                conn.exec(sql).await?;
            }
        }

        info!("====setup done====");

        Ok(())
    }

    async fn begin_insertion(&self) -> Result<JoinHandle<Result<()>>> {
        let conn = self.new_connection_with_test_db().await?;
        let sql = "insert into base select * from rand limit 1";
        let stop_flag = self.stop_flag.clone();
        let handle = tokio::spawn(async move {
            while !stop_flag.load(Ordering::Relaxed) {
                if let Err(e) = conn.exec(sql).await {
                    info!("Insertion err: {e}");
                }
            }

            Ok::<_, anyhow::Error>(())
        });
        Ok(handle)
    }

    async fn begin_compaction(&self) -> Result<JoinHandle<Result<u32>>> {
        let conn = self.new_connection_with_test_db().await?;
        let sql = "optimize table base compact";
        let stop_flag = self.stop_flag.clone();
        let handle = tokio::spawn(async move {
            let mut success_compaction = 0;
            while !stop_flag.load(Ordering::Relaxed) {
                if let Err(e) = conn.exec(sql).await {
                    info!("table compaction err: {e}");
                } else {
                    success_compaction += 1;
                }
            }

            Ok::<_, anyhow::Error>(success_compaction)
        });
        Ok(handle)
    }

    async fn final_consume_all_streams(&self) -> Result<()> {
        let conn = self.new_connection_with_test_db().await?;
        // consume all the derived streams
        for idx in 0..self.args.num_derived_streams {
            let sql = format!("insert into sink_{idx}  select * from base_stream_{idx}");
            conn.exec(&sql).await?;
        }

        // consume the base stream
        let sql = "insert into sink select * from base_stream";
        conn.exec(sql).await?;
        Ok(())
    }

    async fn consume_derived_streams(self: &Arc<Self>) -> Result<Vec<JoinHandle<Result<u32>>>> {
        let mut handles = Vec::new();
        for idx in 0..self.args.num_derived_streams {
            let s = self.clone();
            let join_handle = tokio::spawn(async move { s.concurrently_consume(idx).await });
            handles.push(join_handle);
        }

        Ok(handles)
    }

    async fn concurrently_consume(&self, stream_id: u32) -> Result<u32> {
        let sql = if stream_id % 2 == 0{
            format!("insert into sink_{stream_id}  select * from base_stream_{stream_id}")
        } else {
            format!("merge into sink_{stream_id} using (select * from base_stream_{stream_id}) as s on 1 <> 1 when matched then update * when not matched then insert *")
        };

        let mut handles = Vec::new();
        for batch_id in 0..self.args.stream_consumption_concurrency {
            let conn = self.new_connection_with_test_db().await?;
            let iters = self.args.times_consumption_per_stream;
            let show_err = self.args.show_stream_consumption_errors;
            let join_handle = tokio::spawn({
                let sql = sql.clone();
                async move {
                    let mut sucess: u32 = 0;
                    let step = (iters / 100).max(1);
                    for i in 0..iters {
                        if let Err(e) = conn.exec(&sql).await {
                            if show_err {
                                info!(
                                    "exec: batch {}, stream {}, iter {},  `{}` failed, {}",
                                    batch_id,
                                    stream_id,
                                    i,
                                    sql,
                                    e.to_string()
                                );
                            }
                        } else {
                            sucess += 1;
                        }

                        if (i + 1) % step == 0 {
                            info!(
                                "exec: batch {}, stream {}, iter {}, progress {:.2}%",
                                batch_id,
                                stream_id,
                                i,
                                (i + 1) as f32 * 100.0 / iters as f32
                            );
                        }
                    }
                    Ok::<_, anyhow::Error>(sucess)
                }
            });
            handles.push(join_handle);
        }
        //join all the handles
        let mut success: u32 = 0;
        for join_handle in handles {
            if let Ok(Ok(s)) = join_handle.await {
                success += s;
            }
        }
        Ok(success)
    }

    async fn verify(&self) -> Result<()> {
        info!("==========================");
        info!("======verify result=======");
        info!("==========================");
        let conn = self.new_connection_with_test_db().await?;

        let row = conn.query_row("select count() from sink").await?;
        let (count, ): (u32, ) = row.unwrap().try_into().unwrap();
        let row = conn.query_row("select sum(c) from sink").await?;
        let (sum, ): (u64, ) = row.unwrap().try_into().unwrap();

        info!("===========================");
        info!("Sink table: row count: {count}");
        info!("Sink table: sum of column `c`: {sum}");
        info!("===========================");
        info!("");

        let mut diverses = Vec::new();

        info!("===========================");

        for idx in 0..self.args.num_derived_streams {
            let row = conn
                .query_row(format!("select count() from sink_{idx}").as_str())
                .await?;
            let (c, ): (u32, ) = row.unwrap().try_into().unwrap();
            let row = conn
                .query_row(format!("select sum(c) from sink_{idx}").as_str())
                .await?;
            let (s, ): (u64, ) = row.unwrap().try_into().unwrap();
            info!(
                "sink of derived stream {}: row count {}, sum {} ",
                idx, c, s
            );
            if count == c && sum == s {
                continue;
            } else {
                diverses.push((idx, c, s));
            }
        }

        info!("===========================");
        info!("");

        if diverses.is_empty() {
            info!("===========================");
            info!("======     PASSED      ====");
            info!("===========================");
        } else {
            info!("===========================");
            info!("======     FAILED      ====");
            info!("===========================");
            for (idx, c, s) in diverses {
                info!("diverse result in sink_{idx}: row count: {c}, sum: {s}");
            }
        }

        Ok(())
    }

    async fn create_base_stream(&self) -> Result<()> {
        let conn = self.new_connection_with_test_db().await?;
        let sql = "create stream base_stream on table base";
        conn.exec(sql).await?;
        Ok(())
    }

    async fn create_derived_streams(&self) -> Result<()> {
        let conn = self.new_connection_with_test_db().await?;
        for idx in 0..self.args.num_derived_streams {
            let sql =
                format!("create stream base_stream_{idx} on table base at (STREAM => base_stream)");
            conn.exec(&sql).await?;
            let sql = format!("create table sink_{idx} like base");
            conn.exec(&sql).await?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    info!("###options###: \n {:#?}", args);

    let mut driver = Driver::new(args);
    if let Ok(dsn) = std::env::var("DATABEND_DSN") {
        driver.dsn = dsn;
    }

    let driver = Arc::new(driver);
    driver.setup().await?;

    // insert some random data (this is optional)
    let conn = driver.new_connection_with_test_db().await?;
    let sql = "insert into base select * from rand limit 10";
    let _ = conn.exec(sql).await?;

    let insertion_handle = driver.begin_insertion().await?;
    let compaction_handle = driver.begin_compaction().await?;

    // create the `base`(line) stream
    //
    // note that
    // Although the stream is likely to be based on a snapshot that have more than 10 rows,
    // the verification phase does not assume the exact state of table that streams are being created on.
    driver.create_base_stream().await?;

    // create derived streams, these streams will be align with the `base` stream
    driver.create_derived_streams().await?;

    // for each derived stream, concurrently consume it
    // by inserting the change-set into sink tables
    let handles = driver.consume_derived_streams().await?;

    let success = driver.wait_stream_consuming(handles).await?;
    let args = &driver.args;

    let total_times_stream_consumption = args.stream_consumption_concurrency
        * args.times_consumption_per_stream
        * args.num_derived_streams;
    info!("###options(recall)###: \n {:#?}", args);
    info!("==========================");
    info!(
        "streams consumption executed so far {}",
        total_times_stream_consumption
    );
    info!("success : {}", success);
    info!("==========================\n");

    // stop insertion and compaction
    // during stopping insertion, there might be extra rows inserted into `base` table, that is OK

    driver.stop_flag.store(true, Ordering::Relaxed);
    insertion_handle.await??;
    let num_success_compaction = compaction_handle.await??;

    info!("===========================");
    info!("success compaction: {num_success_compaction}");
    info!("==========================");
    info!("");

    // final consume
    //
    // since the insertion is stopped, after consuming `base` stream and the derived streams
    // the sink tables will be the same
    driver.final_consume_all_streams().await?;

    // verify
    driver.verify().await?;

    Ok(())
}
