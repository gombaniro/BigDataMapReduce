#!/bin/bash

# Generate current date and time in YYYY-MM-DD_HH-MM-SS format
CURRENT_DATE_TIME=$(date +'%Y-%m-%d_%H-%M-%S')

# Specify input and output directories with current date and time
INPUT_DIR="/user/input/access_log"
OUTPUT_DIR="/user/output/im_access_per_ip_$CURRENT_DATE_TIME"

# Run the Hadoop job
hadoop jar  Big-data-mapred-1.0-SNAPSHOT.jar org.bigdata.AverageComputationInMapper $INPUT_DIR "$OUTPUT_DIR"

# Check the exit status of the Hadoop job
# shellcheck disable=SC2181
if [ $? -eq 0 ]; then
  echo "Job(Average computation in mapper) completed successfully."
  echo "The results files are saved :"
  hdfs dfs -ls "$OUTPUT_DIR"
else
  echo "Job (Average computation in mapper) failed."
fi