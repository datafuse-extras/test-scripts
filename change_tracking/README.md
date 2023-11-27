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
         -h, --help
             Print help
   ~~~

