use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;
use databend_driver::{Client, Connection};
use log::info;
use tokio::task::JoinHandle;

/// Auto Vacuum Testing Script - Tests for table corruption with small DATA_RETENTION_NUM_SNAPSHOTS_TO_KEEP values
/// - See issue: https://github.com/databendlabs/databend/issues/18006
/// - This case should fail in databend version https://github.com/databendlabs/databend/releases/tag/v1.2.743-nightlyhhhhhhhhh
#[derive(Parser, Clone, Debug)]
pub struct Args {
    /// Number of concurrent insertion threads
    #[arg(long, default_value_t = 10)]
    concurrency: u32,

    /// Number of insert operations per thread per iteration
    #[arg(long, default_value_t = 20)]
    inserts_per_iteration: u32,

    /// Number of rows to insert in each operation
    #[arg(long, default_value_t = 10)]
    insert_batch_size: u32,

}

#[derive(Clone)]
pub struct AutoVacuumSuite {
    args: Args,
    dsn: String,
}

impl AutoVacuumSuite {
    fn new(args: Args, dsn: String) -> Self {
        Self { args, dsn }
    }

    async fn new_setup_connection(&self) -> Result<Box<dyn Connection>> {
        let client = Client::new(self.dsn.clone());
        let conn = client.get_conn().await?;
        Ok(conn)
    }

    async fn new_connection(&self) -> Result<Box<dyn Connection>> {
        let conn = self.new_setup_connection().await?;
        conn.exec("set enable_auto_vacuum=1").await?;
        conn.exec("use auto_vacuum").await?;
        Ok(conn)
    }

    async fn setup(&self) -> Result<()> {
        info!("===== Running setup for auto vacuum test =====");

        let conn = self.new_setup_connection().await?;

        // Create test table with small DATA_RETENTION_NUM_SNAPSHOTS_TO_KEEP
        let setup_sqls = [
            "create or replace database auto_vacuum",
            "use auto_vacuum",
            "CREATE OR REPLACE TABLE test (
                id DECIMAL(38, 0) NOT NULL,
                a VARIANT NULL,
                b VARCHAR NULL,
                c TIMESTAMP NULL DEFAULT CAST(now() AS Timestamp NULL),
                d TIMESTAMP NULL,
                e DECIMAL(38, 0) NULL DEFAULT 0,
                f VARCHAR NULL,
                g VARCHAR NULL,
                h VARCHAR NULL
            ) CLUSTER BY linear(id)
              BLOCK_SIZE_THRESHOLD='419430400'
              CLUSTER_TYPE='linear'
              COMPRESSION='zstd'
              DATA_RETENTION_NUM_SNAPSHOTS_TO_KEEP='3'",
            "CREATE OR REPLACE TABLE r LIKE test ENGINE = random",
        ];

        for sql in setup_sqls {
            info!("Executing setup SQL: {}", sql);
            conn.exec(sql).await?;
        }

        info!("===== Setup completed =====");
        Ok(())
    }

    async fn execute_insert(&self, batch_id: u32) -> Result<()> {
        let conn = self.new_connection().await?;
        let sql = format!(
            "INSERT INTO test SELECT * FROM r LIMIT {}",
            self.args.insert_batch_size
        );

        for i in 0..self.args.inserts_per_iteration {
            info!("\n===== Batch {batch_id} Iteration {i} Progress {}% =====", i * 100 / self.args.inserts_per_iteration);
            match conn.exec(&sql).await {
                Ok(_) => {
                    info!("INSERT completed successfully");
                }
                Err(e) => {
                    info!("INSERT error: {}", e);
                    return Err(anyhow!("INSERT failed: {}", e));
                }
            }
        }

        Ok(())
    }

    async fn check_table_health(&self) -> Result<bool> {
        let conn = self.new_connection().await?;
        let sql = "SELECT * FROM test ignore_result";

        match conn.exec(sql).await {
            Ok(_) => {
                info!("Table health check passed");
                Ok(true)
            }
            Err(e) => {
                info!("ERROR: Table health check failed: {}", e);
                Ok(false)
            }
        }
    }

    async fn run_concurrent_inserts(&self) -> Result<Vec<JoinHandle<Result<()>>>> {
        let mut handles = Vec::new();

        for i in 0..self.args.concurrency {
            let self_clone = Arc::new(self.clone());
            let handle = tokio::spawn(async move { self_clone.execute_insert(i).await });
            handles.push(handle);
        }

        Ok(handles)
    }

    async fn wait_for_completion(&self, handles: Vec<JoinHandle<Result<()>>>) -> Result<()> {
        for handle in handles {
            let _ = handle.await??;
        }
        Ok(())
    }

    pub async fn run(args: Args, dsn: String) -> Result<()> {
        let suite = Self::new(args, dsn);
        suite.setup().await?;

        // Run concurrent inserts
        let handles = suite.run_concurrent_inserts().await?;
        suite.wait_for_completion(handles).await?;
        // Check table health
        if !suite.check_table_health().await? {
            return Err(anyhow!("Table health check failed. Test terminated."));
        }

        Ok(())
    }
}

pub async fn run(args: Args, dsn: String) -> Result<()> {
    AutoVacuumSuite::run(args, dsn).await
}
