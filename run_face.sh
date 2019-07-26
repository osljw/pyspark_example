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
    num_executors=20
else
    num_executors=5
fi

if [[ $run_mode == "local" ]]; then
    face_files=file://$WK_DIR/../${track}/${track}_face_attrs.txt
    output_dir=file://$WK_DIR/../${track}/${track}_face
    rm -r ${output_dir:7} || true
else
    face_files=<hdfs_path>/${track}/${track}_face_attrs.txt
    output_dir=<hdfs_path>/${track}/${track}_face
    hdfs dfs -rm -r $output_dir || true
fi

echo "=========================="
echo "run_mode=$run_mode"
echo "face_files=$face_files"
echo "=========================="
if [[ $run_mode == "local" ]]; then
    spark-submit \
        --master local \
        extract_face.py \
            --face_files=$face_files \
            --output_dir=$output_dir
else
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors $num_executors \
        extract_face.py \
            --face_files=$face_files \
            --output_dir=$output_dir
fi
