#!/bin/bash
called_file=${BASH_SOURCE[0]}
file_abs_path=`readlink -f $called_file`
_DIR=`dirname $file_abs_path`
###################################################
## Config workspace dir
WK_DIR=$_DIR
cd $WK_DIR

set -e
set -u

export PATH=~/bin:~/hadoop/bin:~/spark-2.4.0-bin-hadoop2.7/bin:$PATH
export SPARK_HOME=~/spark-2.4.0-bin-hadoop2.7

#######################
# $1 - track1 or track2
# $2 - group_name
# $3 - local or spark
#######################
track=$1
group_name=$2
run_mode=$3

if [[ $track == "track1" ]]; then
    num_executors=20
    executor_cores=4
else
    num_executors=5
fi

if [[ $run_mode == "local" ]]; then
    data_files=file://$WK_DIR/../${track}/final_${track}_train_new/partition_day=[^89]/part-*
    output_dir=file://$WK_DIR/../${track}/${track}_${group_name}
    rm -r ${output_dir:7} || true
else
    data_files=<hdfs_path>/${track}/final_${track}_train_new/partition_day=[^89]/part-*
    output_dir=<hdfs_path>/${track}/${track}_${group_name}
    hdfs dfs -rm -r $output_dir || true
fi

echo "=========================="
echo "run_mode=$run_mode"
echo "group_name=$group_name"
echo "data_files=$data_files"
echo "output_dir=$output_dir"
echo "=========================="
if [[ $run_mode == "local" ]]; then
    spark-submit \
        --master local \
        --conf "spark.pyspark.python=/home/appops/Python/bin/python" \
        --conf "spark.pyspark.driver.python=/home/appops/Python/bin/python" \
        --archives pyarrow.zip#pyarrow \
        extract_group.py \
            --data_files=$data_files \
            --group_name=$group_name \
            --output_dir=$output_dir
else
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors $num_executors \
        --executor-cores $executor_cores \
        --executor-memory 1G \
        --archives pyarrow.zip#pyarrow \
        extract_group.py \
            --data_files=$data_files \
            --group_name=$group_name \
            --output_dir=$output_dir
fi
