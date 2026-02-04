#!/bin/bash

# --- CONFIGURATION ---
HDFS_BASE="amazon-review-analytics/big-data-pipeline/data"
HDFS_RAW="$HDFS_BASE/raw"
HDFS_CLEANED="$HDFS_BASE/cleaned"

# Local Paths
LOCAL_RAW_DIR="$HOME/projects/AmazonReviewAnalytics/BigDataPipeline/data/raw"

# --- 1. CLEANUP RAW & CLEANED DATA ---
echo "Cleaning up existing HDFS structures..."

# Delete individual raw files if they exist
hdfs dfs -rm -f $HDFS_RAW/reviews/Appliances.json
hdfs dfs -rm -f $HDFS_RAW/metadata/meta_Appliances.json

# Check and delete the entire 'cleaned' directory if it exists
if hdfs dfs -test -d $HDFS_CLEANED; then
    echo "Deleting existing cleaned directory..."
    hdfs dfs -rm -r $HDFS_CLEANED
fi

# --- 2. CREATE DIRECTORY STRUCTURE ---
echo "Ensuring HDFS directory structure exists..."
hdfs dfs -mkdir -p $HDFS_RAW/reviews
hdfs dfs -mkdir -p $HDFS_RAW/metadata
hdfs dfs -mkdir -p $HDFS_CLEANED/reviews
hdfs dfs -mkdir -p $HDFS_CLEANED/metadata

# --- 3. UPLOAD FILES ---
echo "Uploading files to HDFS..."
hdfs dfs -put $LOCAL_RAW_DIR/Appliances.json $HDFS_RAW/reviews/
hdfs dfs -put $LOCAL_RAW_DIR/meta_Appliances.json $HDFS_RAW/metadata/

# --- 4. VERIFICATION ---
echo "---------------------------------------------------"
echo "HDFS Structure Verified:"
hdfs dfs -ls -R amazon-review-analytics
