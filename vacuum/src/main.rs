use std::fs::read_to_string;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use databend_driver::Client;
use env_logger::Env;
use futures_util::StreamExt;
use log::info;

/// Vacuum Testing Script
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = 5)]
    insertion_concurrency: u32,
    #[arg(long, default_value_t = 1000)]
    insertion_iteration: u32,
    #[arg(long, default_value_t = 5)]
    vacuum_concurrency: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    info!("###options###: \n {:#?}", args);
    let dsn = match std::env::var("DATABEND_DSN") {
        Ok(v) => v,
        Err(_) => {
            let default_dsn =
                "databend://root:@localhost:8000/default?sslmode=disable&retention_period=0"
                    .to_string();
            info!("using default dsn {}", default_dsn);
            default_dsn
        }
    };

    setup(&dsn).await?;

    let (success_inserts, success_vacuum) = execute(&dsn, &args).await?;

    verify(&dsn, success_inserts, success_vacuum).await?;

    Ok(())
}

// read test sql script from file, and execute it
async fn setup(dsn: &str) -> Result<()> {
    info!("=====running setup script====");

    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();
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

async fn execute(dsn: &str, args: &Args) -> Result<(u32, u32)> {
    info!("=====running test script ====");

    let mut insertions = Vec::with_capacity(args.insertion_concurrency as usize);
    for batch_id in 0..args.insertion_concurrency {
        info!("spawning insert batch : {}", batch_id);
        let insert_join_handle = tokio::spawn({
            let dsn = dsn.to_string();
            let args = args.clone();
            async move {
                let mut num_of_success = 0;
                let step = (args.insertion_iteration / 100).max(1);
                for iteration in 0..args.insertion_iteration {
                    let success = exec_insertion(&dsn, batch_id).await?;

                    if (iteration + 1) % step == 0 {
                        info!(
                            "batch {}, executed {}, progress {:.2}%",
                            batch_id,
                            iteration,
                            (iteration + 1) as f32 * 100.0 / args.insertion_iteration as f32
                        );
                    }
                    if success {
                        num_of_success += 1;
                    }
                }
                Ok::<_, anyhow::Error>(num_of_success)
            }
        });
        insertions.push(insert_join_handle);
    }

    let shutdown = Arc::new(AtomicBool::new(false));

    let mut vacuums = Vec::with_capacity(args.vacuum_concurrency as usize);

    // concurrent vacuum tasks
    for batch_id in 0..args.vacuum_concurrency {
        info!("spawning vacuum tasks");
        let vacuum_join_handle = tokio::spawn({
            let dsn = dsn.to_string();
            let shutdown = shutdown.clone();
            async move {
                let mut executed = 0;
                let mut succeed = 0;
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        info!(
                            "vacuum batch : {}, executed {}, succeed {}",
                            batch_id, executed, succeed
                        );
                        break;
                    }

                    if exec_vacuum(&dsn).await.is_ok() {
                        succeed += 1;
                    }

                    executed += 1;

                    if executed % 100 == 0 {
                        info!(
                            "vacuum batch : {}, executed {}, succeed {}",
                            batch_id, executed, succeed
                        );
                    }
                }
                Ok::<_, anyhow::Error>(succeed)
            }
        });
        vacuums.push(vacuum_join_handle);
    }

    // collect the number of successfully executed insertions
    let mut success_insertion = 0;
    for insert_join_handle in insertions {
        if let Ok(Ok(v)) = insert_join_handle.await {
            success_insertion += v
        }
    }

    // then we shutdown the vacuum tasks
    shutdown.store(true, Ordering::Relaxed);

    let mut success_vacuum = 0;
    for x in vacuums {
        if let Ok(Ok(succeed)) = x.await {
            success_vacuum += succeed
        }
    }

    Ok((success_insertion, success_vacuum))
}

async fn exec_insertion(dsn: &str, batch_id: u32) -> Result<bool> {
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await?;

    let val = batch_id * 2;
    let sql = format!("insert into test values({batch_id}, {val})");
    match conn.exec(&sql).await {
        Ok(_) => {
            //   info!("Ok. insert batch : {}", batch_id);
            Ok(true)
        }
        Err(e) => {
            // replace may be failed due to concurrent mutations (compact, purge, recluster)
            info!("Err. insert batch : {}. {e}", batch_id);
            Ok(false)
        }
    }
}

async fn exec_vacuum(dsn: &str) -> Result<()> {
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await?;
    let sql = "vacuum table test RETAIN 0 HOURS";
    conn.exec(sql).await?;
    Ok(())
}

async fn verify(dsn: &str, success_insertions: u32, success_vacuum: u32) -> Result<()> {
    info!("==========================");
    info!("====verify table state====");
    info!("==========================");
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    info!("                           ");
    info!("                           ");
    info!(
        "number of successfully executed insert-into statements : {}",
        success_insertions
    );
    info!(
        "number of successfully executed vacuum statements : {}",
        success_vacuum
    );
    info!("                           ");
    info!("                           ");

    // - check the table data match the number of successfully executed replace into statements
    {
        let row = conn.query_row("select count() from test").await?;
        let count: (u32,) = row.unwrap().try_into().unwrap();
        info!(
            "CHECK: value of successfully executed insert-into statements: client {}, server {}",
            success_insertions, count.0
        );
    }

    // - full table scan, ensure that the table data is not damaged
    info!("CHECK: full table scanning");
    {
        let rows = conn.query_iter("select * from test ignore_result").await;
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
        let (metric, value): (String, String) = r?.try_into().unwrap();
        info!("{metric} : {value}");
    }
    info!("===========================");

    Ok(())
}
