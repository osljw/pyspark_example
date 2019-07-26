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
# $2 - local or spark
#######################
track=$1
data_type=$2
run_mode=$3

if [[ $track == "track1" ]]; then
    num_executors=10
    executor_cores=2
else
    num_executors=5
fi

if [[ $data_type == "train" ]]; then
    input_data_type=train
    partition_day=8
elif [[ $data_type == "eval" ]]; then
    input_data_type=train
    partition_day=9
else
    input_data_type=test
    partition_day=*
fi


if [[ $run_mode == "local" ]]; then
    data_files=file://$WK_DIR/../${track}/final_${track}_${input_data_type}_new/partition_day=$partition_day/part-*
    uid_files=file://$WK_DIR/../${track}/${track}_uid
    output_dir=file://$WK_DIR/../${track}/${track}_${data_type}
    rm -r ${output_dir:7} || true
else
    data_files=<hdfs_path>/${track}/final_${track}_${input_data_type}_new/partition_day=$partition_day/part-*
    uid_files=<hdfs_path>/${track}/${track}_uid
    item_id_files=<hdfs_path>/${track}/${track}_item_id
    author_id_files=<hdfs_path>/${track}/${track}_author_id
    item_city_files=<hdfs_path>/${track}/${track}_item_city
    output_dir=<hdfs_path>/${track}/${track}_${data_type}
    hdfs dfs -rm -r $output_dir || true
fi

echo "=========================="
echo "run_mode=$run_mode"
echo "data_files=$data_files"
echo "uid_files=$uid_files"
echo "output_dir=$output_dir"
echo "=========================="
if [[ $run_mode == "local" ]]; then
    spark-submit \
        --master local \
        --conf "spark.pyspark.python=/home/appops/Python/bin/python" \
        --conf "spark.pyspark.driver.python=/home/appops/Python/bin/python" \
        --archives pyarrow.zip#pyarrow \
        ml_data.py \
            --data_files=$data_files \
            --uid_files=$uid_files \
            --output_dir=$output_dir
else
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors $num_executors \
        --executor-cores $executor_cores \
        --executor-memory 10G \
        --archives pyarrow.zip#pyarrow \
        ml_data.py \
            --data_files=$data_files \
            --uid_files=$uid_files \
            --item_id_files=$item_id_files \
            --author_id_files=$author_id_files \
            --item_city_files=$item_city_files \
            --output_dir=$output_dir
fi
