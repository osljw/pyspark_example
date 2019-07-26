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
which spark-submit
#######################
# $1 - track1 or track2
# $2 - local or spark
#######################
track=$1
run_mode=$2

if [[ $track == "track1" ]]; then
    num_executors=30
else
    num_executors=5
fi

if [[ $run_mode == "local" ]]; then
    train_files=file://$WK_DIR/../${track}/final_${track}_train_withid.txt
    test_files=file://$WK_DIR/../${track}/final_${track}_test_withid.txt
    title_files=file://$WK_DIR/../${track}/${track}_title
    face_files=file://$WK_DIR/../${track}/${track}_face
    video_files=file://$WK_DIR/../${track}/${track}_video
    audio_files=file://$WK_DIR/../${track}/${track}_audio

    train_output_dir=file://$WK_DIR/../${track}/final_${track}_train_new
    test_output_dir=file://$WK_DIR/../${track}/final_${track}_test_new
    dict_output_dir=file://$WK_DIR/../${track}/${track}_dict

    rm -r ${train_output_dir:7} || true
    rm -r ${test_output_dir:7} || true
    rm -r ${dict_output_dir:7} || true
else
    train_files=<hdfs_path>/${track}/final_${track}_train_withid.txt
    test_files=<hdfs_path>/${track}/final_${track}_test_withid.txt
    title_files=<hdfs_path>/${track}/${track}_title
    face_files=<hdfs_path>/${track}/${track}_face
    video_files=<hdfs_path>/${track}/${track}_video
    audio_files=<hdfs_path>/${track}/${track}_audio

    dict_output_dir=<hdfs_path>/${track}/${track}_dict
    train_output_dir=<hdfs_path>/${track}/final_${track}_train_new
    test_output_dir=<hdfs_path>/${track}/final_${track}_test_new

    hdfs dfs -rm -r $train_output_dir || true
    hdfs dfs -rm -r $test_output_dir || true
    hdfs dfs -rm -r $dict_output_dir || true
fi

echo "=========================="
echo "run_mode=$run_mode"
echo "track=$track"
echo "train_files=$train_files"
echo "test_files=$test_files"
echo "title_files=$title_files"
echo "face_files=$face_files"
echo "video_files=$video_files"
echo "audio_files=$audio_files"
echo ""
echo "train_output_dir=$train_output_dir"
echo "test_output_dir=$test_output_dir"
echo "dict_output_dir=$dict_output_dir"
echo "=========================="

if [[ $run_mode == "local" ]]; then
    spark-submit \
        --master local \
        --conf "spark.pyspark.python=/home/appops/Python/bin/python" \
        --conf "spark.pyspark.driver.python=/home/appops/Python/bin/python" \
        --archives pyarrow.zip#pyarrow \
        data_join.py \
            --train_files=$train_files \
            --test_files=$test_files \
            --title_files=$title_files \
            --face_files=$face_files \
            --video_files=$video_files \
            --audio_files=$audio_files \
            --train_output_dir=$train_output_dir \
            --test_output_dir=$test_output_dir \
            --dict_output_dir=$dict_output_dir
else
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors $num_executors \
        --executor-memory 5G \
        --archives pyarrow.zip#pyarrow \
        data_join.py \
            --train_files=$train_files \
            --test_files=$test_files \
            --title_files=$title_files \
            --face_files=$face_files \
            --video_files=$video_files \
            --audio_files=$audio_files \
            --train_output_dir=$train_output_dir \
            --test_output_dir=$test_output_dir \
            --dict_output_dir=$dict_output_dir
fi

        #--conf "spark.pyspark.python=Python/bin/python" \
        #--conf "spark.pyspark.driver.python=Python/bin/python" \
        #--archives Python.zip#Python \
