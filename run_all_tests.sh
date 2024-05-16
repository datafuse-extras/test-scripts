#!/bin/bash

ERROR_COUNT=0

TEST_DIRS=("change_tracking" "txn" "multi-table-insert")

for TEST_DIR in "${TEST_DIRS[@]}"; do
  echo "Running test in $TEST_DIR..."
  (
    cd "./$TEST_DIR" || exit
    RUST_LOG="info,databend_driver=error,databend_client=error" cargo run -r
  )
  
  if [ $? -ne 0 ]; then
    echo "Test in $TEST_DIR failed."
    ERROR_COUNT=$((ERROR_COUNT + 1))
  else
    echo "Test in $TEST_DIR succeeded."
  fi
done

if [ $ERROR_COUNT -ne 0 ]; then
  echo "=================="
  echo "Some tests failed."
  echo "=================="
  exit 1
else
  echo "================="
  echo "All tests passed."
  echo "================="
  exit 0
fi

