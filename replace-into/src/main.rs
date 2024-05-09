use std::fs::read_to_string;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
        .unwrap_or("databend://root:@localhost:8000/default?sslmode=disable&enable_experimental_merge_into=1".to_owned());

    info!("using DSN: {}", dsn);

    let default_number_of_iteration = 100;
    let iterations = if let Some(num_of_iteration) = std::env::args().nth(1) {
        num_of_iteration.parse::<u32>().expect("invalid number")
    } else {
        default_number_of_iteration
    };

    info!("number of iterations to run: {}", iterations);

    setup(&dsn).await?;

    let success_replace_stmts = execute(&dsn, iterations).await?;

    verify(&dsn, success_replace_stmts).await?;

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

    let replace_handle = tokio::spawn({
        let dsn = dsn.to_string();
        async move {
            let mut num_of_success = 0;
            for batch_id in 0..iterations {
                info!("executing batch: {}", batch_id);
                let success = exec_replace(&dsn, batch_id).await?;
                if success {
                    num_of_success += 1;
                }

                if (batch_id + 1) % 7 == 0 {
                    // introduce more conflicts if possible in on replace into stmt
                    let ids = vec![batch_id, batch_id / 2, batch_id / 3];
                    exec_replace_conflict(&dsn, &ids).await?;
                }
            }
            Ok::<_, anyhow::Error>(num_of_success)
        }
    });

    let shutdown = Arc::new(AtomicBool::new(false));

    // background tasks to maintain the table
    let maintain_handle = tokio::spawn({
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

async fn exec_replace_conflict(dsn: &str, batch_ids: &[u32]) -> Result<bool> {
    let conn = new_connection(dsn)?;
    let ids = batch_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    info!("executing replace (with conflict) : [{}]", ids);

    // generate sub query which combine all the data that generated by history batch ids

    let mut sub_query = "select * from test_order where ".to_string();
    let filter = batch_ids
        .iter()
        .map(|id| format!("id1 = {}", id))
        .collect::<Vec<_>>()
        .join(" or ");
    sub_query.push_str(&filter);

    // replace these history data into the table (itself). while table being compacted and re-clustered
    // this may lead to partial and total block update.
    let sql = format!("replace into test_order on(id, insert_time) ({sub_query})");

    match conn.exec(&sql).await {
        Ok(_) => {
            info!("Ok. replace batch (with conflict) : [{}]", ids);
            Ok(true)
        }
        Err(e) => {
            // replace may be failed due to concurrent mutations (compact, purge, recluster)
            info!("Err. replace batch (with conflict) : [{}]. {e}", ids);
            Ok(false)
        }
    }
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
          "
    );
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
        //"select * from test_order ignore_result",
        "optimize table test_order compact segment",
        "optimize table test_order compact",
        "optimize table test_order purge",
        "alter table test_order recluster",
    ];
    for sql in sqls {
        match conn.exec(sql).await {
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

    info!("                           ");
    info!("                           ");
    info!(
        "number of successfully executed replace-into statements : {}",
        success_replace_stmts
    );
    info!("                           ");
    info!("                           ");

    // - check the table data match the number of successfully executed replace into statements
    {
        info!("CHECK: value of successfully executed replace into statements");

        // For most of the cases, there should be 1000 * success_replace_stmts rows
        //
        // but in a client/server setting, one can not assume that the client always agree with the server
        // if a statement is successfully executed on the server (e.g. conmmunication failure).
        //
        // thus we just show the number, instead of asserting they are equal.

        let mut rows = conn.query_iter("select count() from test_order").await?;
        let r = rows.next().await.unwrap().unwrap();
        let count: (u32,) = r.try_into()?;
        info!(
            "CHECK: value of successfully executed replace into statements: client {}, server {}",
            success_replace_stmts * 1000,
            count.0
        );

        // CHECK: batch size of successfully executed replace into statements

        // for each unique id2, the count should be 1000 (even if there are communication failures)
        let mut rows = conn
            .query_iter(
                "
            select count() from
                (select count() a, id1 from test_order  group by id1)
                where a != 1000",
            )
            .await?;
        let r = rows.next().await.unwrap().unwrap();
        let count: (u32,) = r.try_into()?;
        assert_eq!(0, count.0);

        // show the number of distinct value of id2
        // not required to be equal, since there might be communication failures
        let mut rows = conn
            .query_iter("select count(distinct(id2)) from test_order")
            .await?;
        let r = rows.next().await.unwrap().unwrap();
        let count: (u32,) = r.try_into()?;

        assert_eq!(success_replace_stmts, count.0);
        info!(
            "CHECK: distinct ids: client {}, server {}",
            success_replace_stmts, count.0
        );
    }

    // - check the value of correlated column
    // for all the rows, id2 should be equal to id1 * 7
    {
        let mut rows = conn
            .query_iter("select count() from test_order where id2 != id1 * 7")
            .await?;
        let r = rows.next().await.unwrap().unwrap();
        let count: (i64,) = r.try_into()?;

        info!("CHECK: value of correlated column");

        assert_eq!(0, count.0);
    }

    // - full table scan, ensure that the table data is not damaged
    info!("CHECK: full table scanning");
    {
        let rows = conn
            .query_iter("select * from test_order ignore_result")
            .await;
        assert!(rows.is_ok(), "full table scan failed");
        let mut rows = rows?;
        while rows.next().await.is_some() {}
    }

    info!("===========================");
    info!("======     PASSED      ====");
    info!("===========================");

    info!("                           ");
    info!("                           ");

    info!("========METRICS============");
    let mut rows = conn.query_iter("select metric, value from system.metrics where metric like '%replace%'  or metric like '%conflict%' order by metric")
        .await?;
    while let Some(r) = rows.next().await {
        let (metric, value): (String, String) = r.unwrap().try_into()?;
        info!("{metric} : {value}");
    }
    info!("===========================");

    info!("                           ");
    info!("                           ");

    info!("======CLUSTERING INFO======");
    let mut rows = conn
        .query_iter("select * from clustering_information('default', 'test_order')")
        .await?;
    while let Some(r) = rows.next().await {
        let (
            cluster_key,
            block_count,
            constant_block_count,
            unclustered_block_count,
            average_overlaps,
            average_depth,
            block_depth_histogram,
        ): (String, u64, u64, u64, f64, f64, String) = r.unwrap().try_into()?;
        info!("cluster_key : {cluster_key}");
        info!("block_count: {block_count}");
        info!("constant_block_count: {constant_block_count}");
        info!("unclustered_block_count: {unclustered_block_count}");
        info!("average_overlaps: {average_overlaps}");
        info!("average_depth: {average_depth}");
        info!("block_depth_histogram: {block_depth_histogram}");
    }
    info!("===========================");

    Ok(())
}
