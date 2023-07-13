use std::fs::read_to_string;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use databend_driver::new_connection;
use env_logger::Env;
use futures_util::StreamExt;
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let dsn = std::env::var("DATABEND_DSN")
        .map_err(|_| {
            "DATABEND_DSN is empty, please EXPORT DATABEND_DSN=<your-databend-dsn>".to_string()
        })
        .unwrap();


    let iterations =
        if let Some(num_of_iteration) = std::env::args().nth(1) {
            num_of_iteration.parse::<u32>().expect("invalid number")
        } else {
            info!("no number of iterations specified, default to 1000");
            1000
        };


    setup(&dsn).await?;

    let success_replace_stmts = execute(&dsn, iterations).await?;

    verify(&dsn, success_replace_stmts).await?;

//    teardown(conn.as_ref()).await?;

    Ok(())
}

// read test sql script from file, and execute it
async fn setup(dsn: &str) -> Result<()> {
    info!("=====running setup script====");

    let conn = new_connection(dsn)?;
    let setup_script = read_to_string("tests/sql/setup.sql")?;

    let sqls = setup_script.split(';');

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

async fn execute(dsn: &str, iterations: u32) -> Result<u32> {
    info!("=====running test script ====");

    let replace_handle =
        tokio::spawn({
            let dsn = dsn.to_string();
            async move {
                let mut num_of_success = 0;
                for batch_id in 0..iterations {
                    let success = exec_replace(&dsn, batch_id).await?;
                    if success {
                        num_of_success += 1;
                    }
                }
                Ok::<_, anyhow::Error>(num_of_success)
            }
        });


    let shutdown = Arc::new(AtomicBool::new(false));

    // background tasks to maintain the table
    let maintain_handle =
        tokio::spawn({
            let dsn = dsn.to_string();
            let shutdown = shutdown.clone();
            async move {
                let mut batch_id = 0;
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    // we do not care if this fails
                    let _ = exec_table_maintenance(&dsn, batch_id).await;
                    batch_id += 1;
                }
                Ok::<_, anyhow::Error>(())
            }
        });

    // join all the join handles

    // wait for replace stmts to finish
    let success_replace_stmts = replace_handle.await??;

    // then we shutdown the table maintenance tasks
    shutdown.store(true, Ordering::Relaxed);

    maintain_handle.await??;


    Ok(success_replace_stmts)
}


async fn exec_replace(dsn: &str, batch_id: u32) -> Result<bool> {
    let conn = new_connection(dsn)?;

    info!("executing replace batch : {}", batch_id);
    let batch_correlated_value = batch_id * 7;
    let sql = format!(
        "
         replace into test_order on(id, insert_time)
          select
                id,
                {batch_id} as id1,
                {batch_correlated_value} as id2,
                id3, id4, id5, id6, id7,
                s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13,
                d1, d2, d3, d4, d5, d6, d7, d8, d9, d10,
                insert_time,
                insert_time1,
                insert_time2,
                insert_time3,
                i
          from random_source limit 1000
          ");
    match conn.exec(&sql).await {
        Ok(_) => {
            info!("Ok. replace batch : {}", batch_id);
            Ok(true)
        }
        Err(e) => {
            // replace may be failed due to concurrent mutations (compact, purge, recluster)
            info!("Err. replace batch : {}. {e}", batch_id);
            Ok(false)
        }
    }
}

async fn exec_table_maintenance(dsn: &str, batch_id: i32) -> Result<()> {
    info!("executing table maintenance batch : {}", batch_id);
    let conn = new_connection(dsn)?;
    let sqls = vec![
        "optimize table test_order compact segment",
        "optimize table test_order compact",
        "optimize table test_order purge",
        "alter table test_order recluster",
    ];
    for sql in sqls {
        match conn.exec(sql)
            .await {
            Ok(_) => {
                info!("Ok. maintenance batch : {}", batch_id);
            }
            Err(e) => {
                info!("Err. maintenance batch : {}. {e}", batch_id);
            }
        }
    }
    Ok(())
}

async fn verify(dsn: &str, success_replace_stmts: u32) -> Result<()> {
    info!("==========================");
    info!("====verify table state====");
    info!("==========================");
    let conn = new_connection(dsn)?;

    // - check the table data match the number of successfully executed replace into statements
    {
        info!("CHECK: value of successfully executed replace into statements");

        // there should be 1000 * success_replace_stmts rows

        let mut rows = conn.query_iter("select count() from test_order")
            .await?;
        let r = rows.next().await.unwrap().unwrap();
        let count: (u32, ) = r.try_into()?;
        assert_eq!(success_replace_stmts * 1000, count.0);

        // for each unique id2, the count should be 1000
        let mut rows = conn.query_iter("
            select count() from
                (select count() a, id1 from test_order  group by id1)
                where a != 1000")
            .await?;
        let r = rows.next().await.unwrap().unwrap();
        let count: (u32, ) = r.try_into()?;
        assert_eq!(0, count.0);


        // there should be success_replace_stmts distinct id2
        let mut rows = conn.query_iter("select count(distinct(id2)) from test_order")
            .await?;
        let r = rows.next().await.unwrap().unwrap();
        let count: (u32, ) = r.try_into()?;

        assert_eq!(success_replace_stmts, count.0);
    }

    // - check the value of correlated column
    // for all the rows, id2 should be equal to id1 * 7
    {
        let mut rows = conn.query_iter("select count() from test_order where id2 != id1 * 7")
            .await?;
        let r = rows.next().await.unwrap().unwrap();
        let count: (i64, ) = r.try_into()?;

        info!("CHECK: value of correlated column");

        assert_eq!(0, count.0);
    }

    // - full table scan, ensure that the table data is not damaged
    info!("CHECK: full table scanning");
    {
        let rows = conn.query_iter("select * from test_order ignore_result")
            .await;
        assert!(rows.is_ok(), "full table scan failed");
        let mut rows = rows?;
        while rows.next().await.is_some() {}
    }


    info!("===========================");
    info!("======     PASSED      ====");
    info!("===========================");

    Ok(())
}

