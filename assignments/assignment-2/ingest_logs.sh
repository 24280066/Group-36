#!/bin/bash

# Ensure a date argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 YYYY-MM-DD"
    exit 1
fi

# Parse date components
DATE=$1
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Define HDFS paths
HDFS_LOGS_DIR="/raw/logs/$YEAR/$MONTH/$DAY"
HDFS_METADATA_DIR="/raw/metadata/$YEAR/$MONTH/$DAY"

# Define local paths
LOCAL_LOGS_PATH="/mnt/c/Users/ABPKSUP/Desktop/raw_data/logs_$DATE.csv"
LOCAL_METADATA_PATH="/mnt/c/Users/ABPKSUP/Desktop/raw_data/metadata.csv"

# Create HDFS directories
hdfs dfs -mkdir -p $HDFS_LOGS_DIR
hdfs dfs -mkdir -p $HDFS_METADATA_DIR

# Copy files to HDFS
hdfs dfs -put $LOCAL_LOGS_PATH $HDFS_LOGS_DIR/
hdfs dfs -put $LOCAL_METADATA_PATH $HDFS_METADATA_DIR/

echo " Data for $DATE successfully ingested into HDFS."
