use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;
use databend_driver::{Client, Connection};
use log::info;
use tokio::task::JoinHandle;
use tokio::time;
use std::sync::atomic::{AtomicBool, Ordering};

/// Vacuum2 Testing Script - Tests for table corruption with concurrent writes and vacuum operations
/// - Tests two scenarios: simple concurrent writes and writes within explicit transactions
#[derive(Parser, Clone, Debug)]
pub struct Args {
    /// Number of concurrent writer threads
    #[arg(long, default_value_t = 10)]
    writers: u32,

    /// Number of concurrent vacuum executor threads
    #[arg(long, default_value_t = 3)]
    vacuumers: u32,

    /// Number of insert operations per thread
    #[arg(long, default_value_t = 20)]
    inserts_per_thread: u32,

    /// Number of rows to insert in each operation
    #[arg(long, default_value_t = 10)]
    insert_batch_size: u32,

    /// Run scenario with explicit transactions
    #[arg(long, default_value_t = false)]
    explicit_txn: bool,
}

#[derive(Clone)]
pub struct Vacuum2Suite {
    args: Args,
    dsn: String,
}

impl Vacuum2Suite {
    fn new(args: Args, dsn: String) -> Self {
        Self { args, dsn }
    }

    async fn new_connection(&self) -> Result<Box<dyn Connection>> {
        let client = Client::new(self.dsn.clone());
        let conn = client.get_conn().await?;
        Ok(conn)
    }

    async fn setup(&self) -> Result<()> {
        info!("===== Running setup for vacuum2 test =====");

        let conn = self.new_connection().await?;

        // Create test database and tables
        let setup_sqls = [
            "CREATE OR REPLACE DATABASE test_vacuum2",
            "USE test_vacuum2",
            "CREATE OR REPLACE TABLE t1 (
                id DECIMAL(38, 0) NOT NULL,
                a VARIANT NULL,
                b VARCHAR NULL,
                c TIMESTAMP NULL DEFAULT CAST(now() AS Timestamp NULL),
                d TIMESTAMP NULL,
                e DECIMAL(38, 0) NULL DEFAULT 0,
                f VARCHAR NULL,
                g VARCHAR NULL,
                h VARCHAR NULL
            )",
            // Create a random table for data generation
            "CREATE OR REPLACE TABLE r LIKE t1 ENGINE = random",
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

        // Set retention period to 0 for more extreme testing
        conn.exec("SET data_retention_time_in_days = 0").await?;
        conn.exec("USE test_vacuum2").await?;

        let sql = format!(
            "INSERT INTO t1 SELECT * FROM r LIMIT {}",
            self.args.insert_batch_size
        );

        if self.args.explicit_txn {
            // Scenario 2: Insert within explicit transaction
            conn.exec("BEGIN").await?;

            for i in 0..self.args.inserts_per_thread {
                info!("\n===== Writer {batch_id} Iteration {i} Progress {}% =====",
                     i * 100 / self.args.inserts_per_thread);

                match conn.exec(&sql).await {
                    Ok(_) => {
                        info!("INSERT within transaction completed successfully");
                    }
                    Err(e) => {
                        info!("INSERT error within transaction: {}", e);
                        // Try to rollback on error but continue with test
                        let _ = conn.exec("ROLLBACK").await;
                        return Ok(());
                    }
                }
            }

            // Commit the transaction
            match conn.exec("COMMIT").await {
                Ok(_) => {
                    info!("Transaction committed successfully");
                }
                Err(e) => {
                    info!("Transaction commit error: {}", e);
                }
            }
        } else {
            // Scenario 1: Simple concurrent inserts
            for i in 0..self.args.inserts_per_thread {
                info!("\n===== Writer {batch_id} Iteration {i} Progress {}% =====",
                     i * 100 / self.args.inserts_per_thread);

                match conn.exec(&sql).await {
                    Ok(_) => {
                        info!("INSERT completed successfully");
                    }
                    Err(e) => {
                        // It is OK if the insert fails, e.g. due to concurrent mutations
                        // But table data should NOT be corrupted, i.e. later the table health check should pass
                        info!("INSERT error: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn execute_vacuum(&self, vacuum_id: u32, running_flag: Arc<AtomicBool>) -> Result<()> {
        let conn = self.new_connection().await?;
        conn.exec("USE test_vacuum2").await?;

        info!("===== Vacuum thread {vacuum_id} starting =====");

        // Keep running vacuum until the running_flag is set to false (when all inserts are done)
        while running_flag.load(Ordering::Relaxed) {
            conn.exec("SET data_retention_time_in_days = 0").await?;
            match conn.exec("CALL system$fuse_vacuum2('test_vacuum2', 't1')").await {
                Ok(_) => {
                    info!("VACUUM iteration completed successfully");
                }
                Err(e) => {
                    info!("VACUUM error: {}", e);
                }
            }
        }

        info!("===== Vacuum thread {vacuum_id} completed =====");
        Ok(())
    }

    async fn check_table_health(&self) -> Result<bool> {
        let conn = self.new_connection().await?;
        conn.exec("USE test_vacuum2").await?;
        let sql = "SELECT * FROM t1 ignore_result";

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

        for i in 0..self.args.writers {
            let self_clone = Arc::new(self.clone());
            let handle = tokio::spawn(async move { self_clone.execute_insert(i).await });
            handles.push(handle);
        }

        Ok(handles)
    }

    async fn run_concurrent_vacuums(&self, running_flag: Arc<AtomicBool>) -> Result<Vec<JoinHandle<Result<()>>>> {
        let mut handles = Vec::new();

        for i in 0..self.args.vacuumers {
            let self_clone = Arc::new(self.clone());
            let running_flag_clone = running_flag.clone();
            let handle = tokio::spawn(async move { self_clone.execute_vacuum(i, running_flag_clone).await });
            handles.push(handle);
        }

        Ok(handles)
    }

    async fn wait_for_completion(&self, handles: Vec<JoinHandle<Result<()>>>) -> Result<()> {
        for handle in handles {
            handle.await??;
        }
        Ok(())
    }

    pub async fn run(args: Args, dsn: String) -> Result<()> {
        let explicit_txn = args.explicit_txn;
        let suite = Self::new(args, dsn);
        suite.setup().await?;

        // Create a flag to signal when inserts are complete
        let running_flag = Arc::new(AtomicBool::new(true));

        // Run concurrent writers and vacuumers
        let scenario_name = if explicit_txn { "explicit transaction" } else { "simple concurrent writes" };
        info!("===== Running vacuum2 test with {} scenario =====", scenario_name);

        let writer_handles = suite.run_concurrent_inserts().await?;
        let vacuum_handles = suite.run_concurrent_vacuums(running_flag.clone()).await?;

        // Wait for all writers to complete
        suite.wait_for_completion(writer_handles).await?;

        // Signal vacuum threads to stop and wait for them to complete
        running_flag.store(false, Ordering::Relaxed);
        suite.wait_for_completion(vacuum_handles).await?;

        // Check table health
        if !suite.check_table_health().await? {
            return Err(anyhow!("Table health check failed. Test terminated."));
        }

        info!("===== Vacuum2 test with {} scenario completed successfully =====", scenario_name);
        Ok(())
    }
}

pub async fn run(args: Args, dsn: String) -> Result<()> {
    Vacuum2Suite::run(args, dsn).await
}
