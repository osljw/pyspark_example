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
    audio_files=file://$WK_DIR/../${track}/${track}_audio_features*.txt
    output_dir=file://$WK_DIR/../${track}/${track}_audio
    rm -r ${output_dir:7} || true
else
    audio_files=<hdfs_path>/${track}/${track}_audio_features*.txt
    output_dir=<hdfs_path>/${track}/${track}_audio
    hdfs dfs -rm -r $output_dir || true
fi

echo "=========================="
echo "run_mode=$run_mode"
echo "audio_files=$audio_files"
echo "output_dir=$output_dir"
echo "=========================="
if [[ $run_mode == "local" ]]; then
    spark-submit \
        --master local \
        extract_audio.py \
            --audio_files=$audio_files \
            --output_dir=$output_dir
else
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors $num_executors \
        --executor-memory 2G \
        extract_audio.py \
            --audio_files=$audio_files \
            --output_dir=$output_dir
fi
