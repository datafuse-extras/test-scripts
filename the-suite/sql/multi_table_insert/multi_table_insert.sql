INSERT FIRST
    WHEN n % 10 = 0 THEN
      INTO t0
    WHEN n % 10 = 1 THEN
      INTO t1
    WHEN n % 10 = 2 THEN
        INTO t2
    WHEN n % 10 = 3 THEN
        INTO t3
    WHEN n % 10 = 4 THEN
        INTO t4
    WHEN n % 10 = 5 THEN
        INTO t5
    WHEN n % 10 = 6 THEN
        INTO t6
    WHEN n % 10 = 7 THEN
        INTO t7
    WHEN n % 10 = 8 THEN
        INTO t8
    WHEN n % 10 = 9 THEN
        INTO t9
SELECT number as n from numbers(10000);