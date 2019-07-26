#!/bin/bash
called_file=${BASH_SOURCE[0]}
file_abs_path=`readlink -f $called_file`
_DIR=`dirname $file_abs_path`
###################################################
cd $_DIR

set -e
set -u

#run_mode=local
run_mode=spark

dt=20181221
log_dt=`date -d "${dt}" +%Y-%m-%d`

if [[ $run_mode == "local" ]]; then
    in_file="file:///home/<local_path>/data_analysis/data/$dt/part-00000"
    rankserver_log_dir="/datastream/<hdfs_path>/$log_dt/server_20181227-235726227+0800.57668066557461661.00000213-1141310018--1193959466.lzo"
    out_dir="file:///home/<localpath>/data_analysis/stat_pctr/output/$dt"
    rm -r $out_dir || true
else
    in_file="/user/<hdfs_path>/$dt/part-*"
    rankserver_log_dir="/datastream/<hdfs_path>/$log_dt"
    out_dir="/user/<hdfs_path>/data_analysis/stat_pctr/$dt"
    hdfs dfs -rm -r $out_dir || true
fi

echo "=========================="
echo "run_mode=$run_mode"
echo "dt=$dt"
echo "in_file=$in_file"
echo "rankserver_log_dir=$rankserver_log_dir"
echo "=========================="

if [[ $run_mode == "local" ]]; then
    spark-submit \
        --master local \
        stat_location_pctr.py \
            --in_file=$in_file \
            --rankserver_log_dir=$rankserver_log_dir \
            --out_dir=$out_dir
else
    zip -j conf.zip conf/*
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors 20 \
        --archives conf.zip#conf \
        stat_location_pctr.py \
            --in_file=$in_file \
            --rankserver_log_dir=$rankserver_log_dir \
            --out_dir=$out_dir
fi
