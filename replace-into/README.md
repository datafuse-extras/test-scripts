# test scenario 


## setup phase

  test table `test_order` will be created. which consists of 36 columns: 
  8 bigint, 13 varchar, 10 decimal, 4 timestamp and 1 int. 
 
  Table `random_source` of the same schema, with `random` engine, will also be created, which will
  be used as the random data source for the test table `test_order`.
  
  
## execution phase

### **`replace into`** routine
    
  **for each iteration, 1000 random rows will be replaced into `test_order`, with the following arrangements**

  - `on conflict` columns specified to 2 columns
   
    one is of type bigint, the other is of type timestamp.
   
  - values of two columns will be set correlated to value of current iteration

  - failures during executing the `replace into` statement will be ignored 
   
    those failures are mainly due to the conflicts of mutations. 
   
    the case driver will record the number of failures thought, so that later in the verification phase, 
    the data can be verified more in detail.


   roughly, the sql looks like the following:
   ~~~.sql
    replace into test_order on(id, insert_time)
       select 
          id,
          {iteration} as id1,
          {iteration * 7} as id2,
          ...
       from random limit 1000; 
   ~~~

 **For each 7 iterations, an extra action will be taken:**

 data of 3 previous iterations will be replaced by them self, e.g.

 ~~~.sql
 replace into test_order on(id, insert_time)
   (select * from test_order where id1 = 27 
     union all select * from test_order where id1 = 13 
     union all select * from test_order where id1 = 9)
 ~~~
 as table data being compacted and re-clustered, this lead to data blocks being partially or totally updated.
   
### table maintenance routine

  table maintenance tasks will be executed **concurrently**, namely the following sqls:

  ~~~.sql
  optimize table test_order compact segment;
  optimize table test_order compact;
  optimize table test_order purge;
  alter table test_order recluster;
  ~~~
  during execution of the above sqls, errors will be ignored (mainly due to the conflicts of mutations). The case driver
  just keep trying executing each of the sqls as soon as possible (to simulate high concurrency scenario).
 
# verification phase

After iterations of `replace into` tasks done, the table maintenance tasks will be stopped.
Then the followings will be verified:

- value of successfully executed replace into statements
 
   1. count of `test_order`should equal to the number of successfully executed `replace into` statements, multiplied by 1000.
  
      **caveat**: there might be false negative in this case
   
      failure of execution the `replace into` statement at **client** side, does not mean that the execution is definitely failed at **server** side(network error, etc).
      
      

   2. count of each distinct value of `id1` should equal to 1000, since for each iteration, 1000 rows are inserted with the same(but distinct) value of `id1`. 


- value of correlated columns 

   for all the rows in `test_order`, value of `id2` should equal to value of `id1` multiplied by 7.
 
- full table scan should work

NOTE:

after the setup phase, at any time, the following invariant should hold:

- `select count() from test_order where id2 != id1 * 7` eq 0
- `select count() from (select count() a, id1 from test_order  group by id1) where a != 1000")` eq 0
- `select * from test_order ignore_result` should work

# how to run

- set DSN 

  Set the environment variable `DATABAEND_DSN` to the connection string of the database to be tested

  e.g. `export DATABEND_DSN="databend://root:@localhost:8000/?sslmode=disable"`

- kick off the test

  `cargo run -r -- [ITERATION]`

   Where the `[ITERATION]` is the number of iterations to run the test for. 
   If not specified, the default is 1000 iterations.

   At the end of execution, if everything is OK, logs like the following should be printed to the console:

    ```
    ==========================
    ====verify table state====
    ==========================
    CHECK: value of successfully executed replace into statements
    CHECK: value of correlated column
    CHECK: full table scanning
    ===========================
    ======     PASSED      ====
    ===========================
    ```

   Otherwise, error messages will be printed to the console.
