use std::fs::read_to_string;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use clap::Parser;
use databend_driver::{Client, Connection};
use log::info;
use tokio::task::JoinHandle;

const SET_UP: &str = "./sql/change_tracking/setup.sql";
const SET_UP_CLUSTERED: &str = "./sql/change_tracking/setup_clustered.sql";

/// Change Tracking Testing Script
#[derive(Parser, Debug)]
pub struct Args {
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

    /// append only or standard stream
    #[arg(long, default_value_t = false)]
    append_only_stream: bool,

    /// append only or standard stream
    #[arg(long, default_value_t = false)]
    clustered_table: bool,
}

pub struct ChangeTrackingSuite {
    args: Args,
    stop_flag: Arc<AtomicBool>,
    dsn: String,
}

impl ChangeTrackingSuite {
    fn new(args: Args, dsn: String) -> Self {
        Self {
            args,
            stop_flag: Arc::new(AtomicBool::new(false)),
            dsn,
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
        let setup_file_path =
            if self.args.clustered_table {
                SET_UP_CLUSTERED
            } else {
                SET_UP
            };
        info!("setup file path {}", setup_file_path);
        let setup_script = read_to_string(setup_file_path)?;

        let db_set_sqls = vec![
            "create or replace database test_stream",
            "use test_stream",
        ];

        let setup_lines = setup_script.split(';');

        let sqls = db_set_sqls.into_iter().chain(setup_lines);

        for sql in sqls {
            let sql = sql.trim();
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
        let sql = "insert into base select a, b, uuid() as c, d from rand limit 100";
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

    async fn begin_delete(&self) -> Result<JoinHandle<Result<()>>> {
        let conn = self.new_connection_with_test_db().await?;
        let sql = "delete from base where a < -15000 and d < '1970-01-01 00:00:00'";
        let stop_flag = self.stop_flag.clone();
        let handle = tokio::spawn(async move {
            while !stop_flag.load(Ordering::Relaxed) {
                if let Err(e) = conn.exec(sql).await {
                    info!("Deletion err: {e}");
                }
            }

            Ok::<_, anyhow::Error>(())
        });
        Ok(handle)
    }

    async fn begin_replace(&self) -> Result<JoinHandle<Result<()>>> {
        let conn = self.new_connection_with_test_db().await?;
        let sql = "replace into base on(a) select a, b, uuid() as c, d from rand limit 2";
        let stop_flag = self.stop_flag.clone();
        let handle = tokio::spawn(async move {
            while !stop_flag.load(Ordering::Relaxed) {
                if let Err(e) = conn.exec(sql).await {
                    info!("Replace err: {e}");
                }
            }

            Ok::<_, anyhow::Error>(())
        });
        Ok(handle)
    }

    async fn begin_update(&self) -> Result<JoinHandle<Result<()>>> {
        let conn = self.new_connection_with_test_db().await?;
        let sql = "update base set d = now() where d > '2099-01-01 00:00:00' and a > 15000";
        let stop_flag = self.stop_flag.clone();
        let handle = tokio::spawn(async move {
            while !stop_flag.load(Ordering::Relaxed) {
                if let Err(e) = conn.exec(sql).await {
                    info!("Update err: {e}");
                }
            }

            Ok::<_, anyhow::Error>(())
        });
        Ok(handle)
    }

    async fn begin_merge(&self) -> Result<JoinHandle<Result<()>>> {
        let conn = self.new_connection_with_test_db().await?;
        let sql = "merge into base using (select a, b, uuid() as c, d from rand limit 10) as s on base.a = s.a \
                        when matched and s.d > '2099-01-01 00:00:00' then update set base.b = s.b and base.d = now() \
                        when matched and s.d < '1970-01-01 00:00:00' then delete when not matched then insert *";
        let stop_flag = self.stop_flag.clone();
        let handle = tokio::spawn(async move {
            while !stop_flag.load(Ordering::Relaxed) {
                if let Err(e) = conn.exec(sql).await {
                    info!("Merge err: {e}");
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

    async fn begin_recluster(&self) -> Result<JoinHandle<Result<u32>>> {
        let conn = self.new_connection_with_test_db().await?;
        let sql = "alter table base recluster";
        let stop_flag = self.stop_flag.clone();
        let handle = tokio::spawn(async move {
            let mut success_recluster = 0;
            while !stop_flag.load(Ordering::Relaxed) {
                if let Err(e) = conn.exec(sql).await {
                    info!("table recluster err: {e}");
                } else {
                    success_recluster += 1;
                }
            }

            Ok::<_, anyhow::Error>(success_recluster)
        });
        Ok(handle)
    }

    async fn final_consume_all_streams(&self) -> Result<()> {
        let conn = self.new_connection_with_test_db().await?;
        let append_only = self.args.append_only_stream;
        // consume all the derived streams
        for idx in 0..self.args.num_derived_streams {
            let sql = if append_only {
                format!("insert into sink_{idx} select a, b, c, d from base_stream_{idx}")
            } else {
                format!("merge into sink_{idx} as t using \
                        (select a, b, c, d, change$action, change$is_update from base_stream_{idx}) as s \
                        on t.c = s.c when matched and s.change$action = 'DELETE' and s.change$is_update=false then delete \
                        when matched and s.change$action = 'INSERT' and s.change$is_update=true then update * \
                        when not matched and s.change$action = 'INSERT' then insert values(s.a, s.b, s.c, s.d)")
            };
            info!("{}", sql);
            conn.exec(&sql).await?;
        }

        // consume the base stream
        let sql = if append_only {
            "insert into sink select a, b, c, d from base_stream"
        } else {
            "merge into sink as t using (select a, b, c, d, change$action, change$is_update from base_stream) as s \
             on t.c = s.c when matched and s.change$action = 'DELETE' and s.change$is_update=false then delete \
             when matched and s.change$action = 'INSERT' and s.change$is_update=true then update * \
             when not matched and s.change$action = 'INSERT' then insert values(s.a, s.b, s.c, s.d)"
        };
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
        let append_only = self.args.append_only_stream;
        let sql = if append_only {
            if stream_id % 2 == 0 {
                format!(
                    "insert into sink_{stream_id}  select a, b, c, d from base_stream_{stream_id}"
                )
            } else {
                format!("merge into sink_{stream_id} using (select a, b, c, d from base_stream_{stream_id}) as s on 1 <> 1 \
                         when matched then update * when not matched then insert *")
            }
        } else {
            format!("merge into sink_{stream_id} as t using \
                    (select a, b, c, d, change$action, change$is_update from base_stream_{stream_id}) as s \
                    on t.c = s.c when matched and s.change$action = 'DELETE' and s.change$is_update=false then delete \
                    when matched and s.change$action = 'INSERT' and s.change$is_update=true then update * \
                    when not matched and s.change$action = 'INSERT' then insert values(s.a, s.b, s.c, s.d)")
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
        let row = conn.query_row("select sum(b) from sink").await?;
        let (sum, ): (u64, ) = row.unwrap().try_into().unwrap();

        info!("===========================");
        info!("Sink table: row count: {count}");
        info!("Sink table: sum of column `b`: {sum}");
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
                .query_row(format!("select sum(b) from sink_{idx}").as_str())
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
            Ok(())
        } else {
            info!("===========================");
            info!("======     FAILED      ====");
            info!("===========================");
            for (idx, c, s) in diverses {
                info!("diverse result in sink_{idx}: row count: {c}, sum: {s}");
            }
            Err(anyhow!("Test Failed"))
        }
    }

    async fn create_base_stream(&self) -> Result<()> {
        let conn = self.new_connection_with_test_db().await?;
        let append_only = self.args.append_only_stream;
        let sql = format!("create stream base_stream on table base append_only = {append_only}");
        conn.exec(&sql).await?;
        Ok(())
    }

    async fn create_derived_streams(&self) -> Result<()> {
        let conn = self.new_connection_with_test_db().await?;
        let append_only = self.args.append_only_stream;
        for idx in 0..self.args.num_derived_streams {
            let sql =
                format!("create stream base_stream_{idx} on table base at (STREAM => base_stream) append_only = {append_only}");
            conn.exec(&sql).await?;
            let sql = format!("create table sink_{idx} like base");
            conn.exec(&sql).await?;
        }
        Ok(())
    }

    pub async fn run(args: Args, dsn: String) -> Result<()> {
        info!("###options###: \n {:#?}", args);

        let driver = ChangeTrackingSuite::new(args, dsn);

        let driver = Arc::new(driver);
        driver.setup().await?;

        let append_only = driver.args.append_only_stream;
        let clustered_base_table = driver.args.clustered_table;

        // insert some random data (this is optional)
        let conn = driver.new_connection_with_test_db().await?;
        let sql = "insert into base select a, b, uuid() as c, d from rand limit 10";
        let _ = conn.exec(sql).await?;

        let insertion_handle = driver.begin_insertion().await?;
        let compaction_handle = driver.begin_compaction().await?;
        let deletion_handle = driver.begin_delete().await?;
        let mut update_handle = None;
        let mut merge_handle = None;
        let mut replace_handle = None;
        if !append_only {
            update_handle = Some(driver.begin_update().await?);
            merge_handle = Some(driver.begin_merge().await?);
            replace_handle = Some(driver.begin_replace().await?);
        }
        let mut recluster_handle =  None;

        if clustered_base_table {
            recluster_handle = Some(driver.begin_recluster().await?);
        };

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
        deletion_handle.await??;
        if !append_only {
            update_handle.unwrap().await??;
            merge_handle.unwrap().await??;
            replace_handle.unwrap().await??;
        }

        if clustered_base_table {
            recluster_handle.unwrap().await??;
        }

        let num_success_compaction = compaction_handle.await??;

        info!("===========================");
        info!("success compaction: {num_success_compaction}");
        info!("==========================");
        info!("");

        // final consume
        //
        // since the insertion is stopped, after consuming `base` stream and the derived streams
        // the sink tables will be the same

        info!("finalizing consuming all streams");
        driver.final_consume_all_streams().await?;

        info!("verifing");
        // verify
        driver.verify().await?;

        Ok(())
    }
}
