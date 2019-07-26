#!/bin/bash

set -e
set -u

hdfs_dict=<hdfs_path>/track2/track2_train_dict
features=`hdfs dfs -ls $hdfs_dict | grep "feature=" | awk -F'=' '{print $NF}'`

mkdir -p dicts
rm dicts/*
for feature in $features
do
    echo "feature: $feature"
    hdfs dfs -getmerge $hdfs_dict/feature=$feature dicts/$feature
done
