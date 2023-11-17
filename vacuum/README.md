# test scenario 

Concurrently execute "insert into" statements on the same test table, while simultaneously running concurrent "vacuum" statements to clean the test table.

After completing the above operations according to specified parameters, check if the status of the test table meets the expectations: Currently, the check is limited to ensuring that a full-table scan of the test table can be completed successfully.


# how to run

- set DSN (optional)

  Set the environment variable `DATABAEND_DSN` to the connection string of the database to be tested

  e.g. `export DATABEND_DSN="databend://root:@localhost:8000/?sslmode=disable"`

  if not set, the default value `databend://root:@localhost:8000/default?sslmode=disable&retention_period=0` will be used.

  NOTE: It is recommended to set `retention_period` to 0, to cover more extreme conditions.

- set ee license (required, since we are tesing ee feature)

  `export QUERY_DATABEND_ENTERPRISE_LICENSE='.......'`

- kick off the test


   `RUST_LOG="info,databend_driver=error,databend_client=error" cargo run -r -- --insertion-iteration 1000`

   ~~~
   Options:
         --insertion-concurrency <INSERTION_CONCURRENCY>  [default: 5]
         --insertion-iteration <INSERTION_ITERATION>      [default: 1000]
         --vacuum-concurrency <VACUUM_CONCURRENCY>        [default: 5]
   ~~~

