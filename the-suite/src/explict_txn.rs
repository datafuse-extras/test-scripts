use std::vec;

use crate::util::ConnectionExt;
use anyhow::Result;
use databend_driver::Client;

pub async fn run(dsn: String) -> Result<()> {
    let client = Client::new(dsn);

    // setup
    {
        let conn = client.get_conn().await.unwrap();
        conn.exec("create or replace database test_txn").await?;
    }

    let c1 = client.get_conn().await.unwrap();
    c1.exec("use test_txn").await?;

    let c2 = client.get_conn().await.unwrap();
    c2.exec("use test_txn").await?;

    let select_t = "SELECT * FROM t ORDER BY c;";

    c1.exec("CREATE OR REPLACE TABLE t(c int);").await?;

    // c1 commit success, because conflict is detected and resolved
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
    assert!(result.is_ok());
    c1.assert_query(select_t, vec![(1,), (2,)]).await;
    c2.assert_query(select_t, vec![(1,), (2,)]).await;

    // rollback
    c1.begin().await?;
    c1.exec("INSERT INTO t VALUES(1);").await?;
    let result = c1.exec("qwerty").await;
    assert!(result.is_err());
    c1.commit().await?;
    c1.assert_query(select_t, vec![(1,), (2,)]).await;
    c2.assert_query(select_t, vec![(1,), (2,)]).await;

    // rollback
    c1.exec("drop table if exists t1;").await?;
    c1.begin().await?;
    c1.exec("INSERT INTO t VALUES(1);").await?;
    let result = c1.exec("select * from t1").await;
    assert!(result.is_err());
    c1.commit().await?;
    c1.assert_query(select_t, vec![(1,), (2,)]).await;
    c2.assert_query(select_t, vec![(1,), (2,)]).await;

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
    assert!(result.is_ok());

    // no conflict, both commit success
    c1.assert_query(select_t, vec![(1,), (2,)]).await;
    c2.assert_query(select_t, vec![(1,), (2,)]).await;
    c1.exec("CREATE OR REPLACE TABLE t1(c int);").await?;
    let select_t1 = "SELECT * FROM t1 ORDER BY c;";

    c1.begin().await?;
    c1.exec("INSERT INTO t VALUES(1);").await?;
    c1.assert_query(select_t, vec![(1,), (1,), (2,)]).await;
    c2.assert_query(select_t, vec![(1,), (2,)]).await;

    c2.begin().await?;
    c2.exec("INSERT INTO t1 VALUES(3);").await?;
    c1.assert_query::<(i32,)>(select_t1, vec![]).await;
    c2.assert_query(select_t1, vec![(3,)]).await;

    c2.commit().await?;
    c1.commit().await?;
    c1.assert_query(select_t, vec![(1,), (1,), (2,)]).await;
    c2.assert_query(select_t, vec![(1,), (1,), (2,)]).await;
    c1.assert_query(select_t1, vec![(3,)]).await;
    c2.assert_query(select_t1, vec![(3,)]).await;

    //------------------------------------------------
    //transaction that consumes stream retry success
    //------------------------------------------------
    c1.exec("create or replace table base(c int);").await?;
    c1.exec("create or replace table target(c int);").await?;

    c1.exec("CREATE or replace STREAM s ON TABLE base APPEND_ONLY=true;")
        .await?;

    c1.begin().await?;
    c1.exec("INSERT INTO base VALUES(1);").await?;
    // First time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;

    c2.begin().await?;
    c2.exec("INSERT INTO base VALUES(2);").await?;
    c2.exec("INSERT INTO target VALUES(3);").await?;
    c2.commit().await?;
    // Second time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;

    c1.exec("Insert into base values(3);").await?;
    // Third time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;
    c1.exec("Insert into target select c from s;").await?;
    let result = c1.commit().await;
    assert!(result.is_ok());
    c1.assert_query("SELECT c FROM s order by c;", vec![(2,), (3,)]).await;
    c2.assert_query("SELECT c FROM target order by c;", vec![(1,), (3,)]).await;

    //----------------------------------------------------------------------------------
    //transaction that consumes stream retry failed due to conflict segment modification
    //----------------------------------------------------------------------------------
    c1.exec("create or replace table base(c int);").await?;
    c1.exec("create or replace table target(c int);").await?;

    c1.exec("CREATE or replace STREAM s ON TABLE base APPEND_ONLY=true;")
        .await?;

    c1.exec("INSERT INTO base VALUES(1);").await?;

    c1.begin().await?;
    // First time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;
    c1.exec("update base set c = 4 where c = 1;").await?;

    c2.begin().await?;
    c2.exec("INSERT INTO base VALUES(2);").await?;
    c2.exec("update base set c = 100 where c = 1;").await?;
    c2.commit().await?;
    // Second time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;

    c1.exec("Insert into target select c from s;").await?;
    let result = c1.commit().await;
    assert!(result.is_err());
    c1.assert_query("SELECT c FROM s order by c;", vec![(2,), (100,)]).await;
    c2.assert_query("SELECT count(*) FROM target;", vec![(0,)]).await;

    //----------------------------------------------------------------------------------
    //transaction that consumes stream retry failed due to consume the same stream
    //----------------------------------------------------------------------------------
    c1.exec("create or replace table base(c int);").await?;
    c1.exec("create or replace table target(c int);").await?;

    c1.exec("CREATE or replace STREAM s ON TABLE base APPEND_ONLY=true;")
        .await?;

    c1.exec("INSERT INTO base VALUES(1);").await?;

    c1.begin().await?;
    // First time query stream s
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;

    c2.begin().await?;
    c2.exec("INSERT INTO base VALUES(2);").await?;
    c2.exec("Insert into target select c from s;").await?;
    c2.commit().await?;
    // c2 commit success, should not affect c1's stream view
    c1.assert_query("SELECT c FROM s;", vec![(1,)]).await;

    c1.exec("Insert into target select c from s;").await?;
    let result = c1.commit().await;
    assert!(result.is_err());
    c1.assert_query("SELECT count(*) FROM s;", vec![(0,)]).await;
    c2.assert_query("SELECT * FROM target order by c;", vec![(1,), (2,)]).await;

    //----------------------------------------------------------------------------------
    // Transaction consumes stream, concurrent non-conflicting commit to stream's base table, then txn commits
    //----------------------------------------------------------------------------------
    c1.exec("create or replace table base1_new(c int);").await?;
    c1.exec("create or replace table base2_new(c int);").await?; // Different table for c1's write
    c1.exec("create or replace table target1_new(c int);").await?;

    c1.exec("CREATE or replace STREAM s1_new ON TABLE base1_new APPEND_ONLY=true;")
        .await?;

    c1.exec("INSERT INTO base1_new VALUES(10);").await?; // Initial data for stream s1_new

    c1.begin().await?;
    // c1 reads from stream s1_new and consumes data
    c1.assert_query("SELECT c FROM s1_new;", vec![(10,)]).await;
    c1.exec("INSERT INTO target1_new SELECT c FROM s1_new;").await?; // c1 consumes (10) from s1_new

    // c2 (another connection) concurrently modifies base1_new
    let c_concurrent = client.get_conn().await.unwrap();
    c_concurrent.exec("use test_txn").await?;
    c_concurrent.begin().await?;
    c_concurrent.exec("INSERT INTO base1_new VALUES(20);").await?; // c2 adds data to base1_new
    c_concurrent.commit().await?; // c2 commits its change to base1_new

    // c1 now does an operation on a *different* table (base2_new)
    c1.exec("INSERT INTO base2_new VALUES(30);").await?;
    
    let result_c1_commit_new1 = c1.commit().await;
    assert!(result_c1_commit_new1.is_ok(), "c1 should commit successfully as its write to base2_new doesn't conflict with c_concurrent's write to base1_new, despite c1 reading s1_new (on base1_new)");

    // Verify final states
    // Stream s1_new should be advanced past (10) due to c1's consumption. It should now show (20) from c_concurrent.
    c1.assert_query("SELECT c FROM s1_new;", vec![(20,)]).await;
    // target1_new should contain (10) consumed by c1
    c1.assert_query("SELECT * FROM target1_new ORDER BY c;", vec![(10,)]).await;
    // base1_new should contain both values
    c1.assert_query("SELECT * FROM base1_new ORDER BY c;", vec![(10,), (20,)]).await;
    // base2_new should contain c1's insert
    c1.assert_query("SELECT * FROM base2_new ORDER BY c;", vec![(30,)]).await;

    //----------------------------------------------------------------------------------
    // Txn (c1) modifies base, reads stream, concurrent commit by c_aux to base, 
    // c1 modifies base again, reads stream, consumes, and commits (with retry)
    //----------------------------------------------------------------------------------
    c1.exec("create or replace table base_s7(c int);").await?;
    c1.exec("create or replace table target_s7(c int);").await?;
    c1.exec("CREATE or replace STREAM s_s7 ON TABLE base_s7 APPEND_ONLY=true;")
        .await?;

    c1.exec("INSERT INTO base_s7 VALUES(1);").await?; // Initial data for stream

    c1.begin().await?;
    c1.exec("INSERT INTO base_s7 VALUES(10);").await?; // c1's first insert
    // c1's view of stream s_s7 should include its own insert (10) and initial (1)
    c1.assert_query("SELECT c FROM s_s7 ORDER BY c;", vec![(1,), (10,)]).await;

    let c_aux_s7 = client.get_conn().await.unwrap(); 
    c_aux_s7.exec("use test_txn").await?;
    c_aux_s7.begin().await?;
    c_aux_s7.exec("INSERT INTO base_s7 VALUES(20);").await?; // c_aux_s7 inserts (20)
    c_aux_s7.commit().await?; // c_aux_s7 commits

    // c1 continues, makes another insert
    c1.exec("INSERT INTO base_s7 VALUES(30);").await?;
    // c1's view of stream s_s7 should include its own inserts (10) and initial (1).
    // c_aux_s7's (20) should not be visible to c1's current transaction's stream view yet.
    c1.assert_query("SELECT c FROM s_s7 ORDER BY c;", vec![(1,), (10,)]).await;

    c1.exec("INSERT INTO target_s7 SELECT c FROM s_s7 WHERE c > 5;").await?;

    let result_c1_commit_s7 = c1.commit().await;
    assert!(result_c1_commit_s7.is_ok(), "c1 should commit successfully after retry");

    // Verify final states
    // After c1's commit (and retry):
    // Base should have 1 (initial), 10 (c1), 20 (c_aux_s7), 30 (c1)
    c1.assert_query("SELECT * FROM base_s7 ORDER BY c;", vec![(1,), (10,), (20,), (30,)]).await;
    // Target should have 10.
    c1.assert_query("SELECT * FROM target_s7 ORDER BY c;", vec![(10,)]).await;
    // 1,10 are consumed, 20,30 are not consumed
    c1.assert_query("SELECT c FROM s_s7 ORDER BY c;", vec![(20,), (30,)]).await;

     //----------------------------------------------------------------------------------
    // Transaction consumes stream, then rolls back due to an error; stream not advanced
    //----------------------------------------------------------------------------------
    c1.exec("create or replace table base_rb(c int);").await?;
    c1.exec("create or replace table target_rb(c int);").await?;
    c1.exec("CREATE or replace STREAM s_rb ON TABLE base_rb APPEND_ONLY=true;")
        .await?;

    c1.exec("INSERT INTO base_rb VALUES(100);").await?;
    c1.exec("INSERT INTO base_rb VALUES(200);").await?;

    // Check initial stream state
    c1.assert_query("SELECT c FROM s_rb ORDER BY c;", vec![(100,), (200,)]).await;

    c1.begin().await?;
    c1.exec("INSERT INTO target_rb SELECT c FROM s_rb WHERE c = 100;").await?;
    c1.assert_query("SELECT * FROM target_rb;", vec![(100,)]).await; // c1's view of target_rb
    
    let err_result = c1.exec("SELECT * FROM non_existent_table_to_cause_error;").await;
    assert!(err_result.is_err(), "Execution should fail to trigger rollback");
    c1.commit().await?;

    // Verify stream s_rb was not advanced, (100) should still be there
    c1.assert_query("SELECT c FROM s_rb ORDER BY c;", vec![(100,), (200,)]).await;
    // Verify target_rb is empty, as the insert should have been rolled back
    c1.assert_query::<(i32,)>("SELECT * FROM target_rb;", vec![]).await;
    // Verify base_rb is unchanged by this transaction
    c1.assert_query("SELECT * FROM base_rb ORDER BY c;", vec![(100,), (200,)]).await;

    // Now, a new transaction (c3) should be able to consume (100)
    let c3 = client.get_conn().await.unwrap();
    c3.exec("use test_txn").await?;
    c3.begin().await?;
    c3.exec("INSERT INTO target_rb SELECT c FROM s_rb WHERE c = 100;").await?;
    assert!(c3.commit().await.is_ok());

    c3.assert_query("SELECT * FROM target_rb;", vec![(100,)]).await;
    c3.assert_query("SELECT count(*) FROM s_rb;", vec![(0,)]).await;

    println!("All tests passed!");
    Ok(())
}
