# test scenario 

1. test table `base` with change tracking enabled
2. keep 
   - inserting data into base table
   - compacting base table
3. create the first `base_stream` based on the `base` table
   - create multiple test streams based on the `base_stream`
4. for each of the test streams 
   
     concurrently consumes the stream, using `insert into` and `merge into` statements, into sink tables, till reached the predefine number of iterations.

5. stop inserting and compacting

6. consume the `base_stream` and all the derived stream again (one time only)

5. check that all the sink tables have same data 


# how to run

- set DSN (optional)

  Set the environment variable `DATABAEND_DSN` to the connection string of the database to be tested

  e.g. `export DATABEND_DSN="databend://root:@localhost:8000/?sslmode=disable&enable_experimental_merge_into=1"`

  if not set, the default value `databend://root:@localhost:8000/default?sslmode=disable&enable_experimental_merge_into=1` will be used.

- before starting query nodes, set ee license (required, since we are tesing ee feature)

  `export QUERY_DATABEND_ENTERPRISE_LICENSE='.......'`


- kick off the test


   `RUST_LOG="info,databend_driver=error,databend_client=error" cargo run -r`

   or specify the options: 

   `RUST_LOG="info,databend_driver=error,databend_client=error" cargo run -r -- -h`

   ~~~
   Change Tracking Testing Script

   Usage: test_stream [OPTIONS]
   
   Options:
         --num-derived-streams <NUM_DERIVED_STREAMS>
             number of derived streams [default: 5]
         --stream-consumption-concurrency <STREAM_CONSUMPTION_CONCURRENCY>
             degree of concurrency that a stream being consumed [default: 3]
         --times-consumption-per-stream <TIMES_CONSUMPTION_PER_STREAM>
             times that a stream should be consumed [default: 10]
         --show-stream-consumption-errors
             show stream consumption errors
         --append-only-stream
             append only or standard stream
         -h, --help
             Print help
   ~~~

- expected result

   ~~~
	[2024-05-09T04:59:04Z INFO  test_stream] exec: batch 0, stream 1, iter 9, progress 100.00%
	[2024-05-09T04:59:04Z INFO  test_stream] exec: batch 2, stream 1, iter 9, progress 100.00%
	[2024-05-09T04:59:04Z INFO  test_stream] exec: batch 1, stream 1, iter 9, progress 100.00%
	[2024-05-09T04:59:04Z INFO  test_stream] ###options(recall)###:
	     Args {
		num_derived_streams: 5,
		stream_consumption_concurrency: 3,
		times_consumption_per_stream: 10,
		show_stream_consumption_errors: false,
	    }
	[2024-05-09T04:59:04Z INFO  test_stream] ==========================
	[2024-05-09T04:59:04Z INFO  test_stream] streams consumption executed so far 150
	[2024-05-09T04:59:04Z INFO  test_stream] success : 51
	[2024-05-09T04:59:04Z INFO  test_stream] ==========================

	[2024-05-09T04:59:04Z INFO  test_stream] ===========================
	[2024-05-09T04:59:04Z INFO  test_stream] success compaction: 41
	[2024-05-09T04:59:04Z INFO  test_stream] ==========================
	[2024-05-09T04:59:04Z INFO  test_stream]
	[2024-05-09T04:59:04Z INFO  test_stream] finalizing consuming all streams
	[2024-05-09T04:59:04Z INFO  test_stream] insert into sink_0  select c from base_stream_0
	[2024-05-09T04:59:04Z INFO  test_stream] insert into sink_1  select c from base_stream_1
	[2024-05-09T04:59:05Z INFO  test_stream] insert into sink_2  select c from base_stream_2
	[2024-05-09T04:59:05Z INFO  test_stream] insert into sink_3  select c from base_stream_3
	[2024-05-09T04:59:05Z INFO  test_stream] insert into sink_4  select c from base_stream_4
	[2024-05-09T04:59:06Z INFO  test_stream] verifing
	[2024-05-09T04:59:06Z INFO  test_stream] ==========================
	[2024-05-09T04:59:06Z INFO  test_stream] ======verify result=======
	[2024-05-09T04:59:06Z INFO  test_stream] ==========================
	[2024-05-09T04:59:06Z INFO  test_stream] ===========================
	[2024-05-09T04:59:06Z INFO  test_stream] Sink table: row count: 29
	[2024-05-09T04:59:06Z INFO  test_stream] Sink table: sum of column `c`: 11967436919
	[2024-05-09T04:59:06Z INFO  test_stream] ===========================
	[2024-05-09T04:59:06Z INFO  test_stream]
	[2024-05-09T04:59:06Z INFO  test_stream] ===========================
	[2024-05-09T04:59:06Z INFO  test_stream] sink of derived stream 0: row count 29, sum 11967436919
	[2024-05-09T04:59:07Z INFO  test_stream] sink of derived stream 1: row count 29, sum 11967436919
	[2024-05-09T04:59:07Z INFO  test_stream] sink of derived stream 2: row count 29, sum 11967436919
	[2024-05-09T04:59:07Z INFO  test_stream] sink of derived stream 3: row count 29, sum 11967436919
	[2024-05-09T04:59:07Z INFO  test_stream] sink of derived stream 4: row count 29, sum 11967436919
	[2024-05-09T04:59:07Z INFO  test_stream] ===========================
	[2024-05-09T04:59:07Z INFO  test_stream]
	[2024-05-09T04:59:07Z INFO  test_stream] ===========================
	[2024-05-09T04:59:07Z INFO  test_stream] ======     PASSED      ====
	[2024-05-09T04:59:07Z INFO  test_stream] =========================== 
   ~~~
