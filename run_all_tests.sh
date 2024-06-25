#!/bin/bash

ERROR_COUNT=0

# cargo run change-tracking --clustered-table
#TEST_TARGETS=("change_tracking" "txn" "multi-table-insert")
TEST_TARGETS=(
              "change-tracking"
              "change-tracking --append-only-stream"
              "change-tracking --clustered-table"
              "change-tracking --clustered-table --append-only-stream"
              "explicit-txn" "multi-table-insert")

for TEST_SUB_COMMAND in "${TEST_TARGETS[@]}"; do
  echo "*******************************"
  echo "Running test : $TEST_SUB_COMMAND..."
  echo "*******************************"
  (
    cd "./the-suite" || exit
    RUST_BACKTRACE=full RUST_LOG="info,databend_driver=error,databend_client=error" cargo run ${TEST_SUB_COMMAND}
  )
  
  if [ $? -ne 0 ]; then
    echo "******************************"
    echo "Test $TEST_SUB_COMMAND failed."
    echo "******************************"
    ERROR_COUNT=$((ERROR_COUNT + 1))
  else
    echo "*******************************"
    echo "Test $TEST_SUB_COMMAND succeeded."
    echo "*******************************"
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

