#!/bin/bash

# Generate current date and time in YYYY-MM-DD_HH-MM-SS format
CURRENT_DATE_TIME=$(date +'%Y-%m-%d_%H-%M-%S')

# Specify input and output directories with current date and time
INPUT_DIR="/user/crystalball/input"
OUTPUT_DIR="/user/crystalball/output/pair_$CURRENT_DATE_TIME"

# Run the Hadoop job
hadoop jar  Big-data-mapred-1.0-SNAPSHOT.jar org.bigdata.PairCrystalBall $INPUT_DIR "$OUTPUT_DIR"

# Check the exit status of the Hadoop job
# shellcheck disable=SC2181
if [ $? -eq 0 ]; then
  echo "Job completed successfully."
  echo "The results files are saved :"
  hdfs dfs -ls "$OUTPUT_DIR"
else
  echo "Job failed."
fi

