import sys 
import os
import yaml
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f


parser = argparse.ArgumentParser()
parser.add_argument(
    '--video_files', required=True, type=str, help='')
parser.add_argument(
    '--output_dir', required=True, type=str, help='')
args = parser.parse_args()

video_files = args.video_files
output_dir = args.output_dir

spark = SparkSession\
    .builder\
    .appName("StatLocationPctr")\
    .getOrCreate()
sc = spark.sparkContext

def parse_video_line(line):
  line = line.strip()
  line_dict = json.loads(line)
  if len(line_dict["video_feature_dim_128"]) == 0:
      return "-", "-"
  item_id = line_dict["item_id"]
  #video_128 = ",".join([str(x) for x in line_dict["video_feature_dim_128"]])
  video_128 = ",".join(["{:.6f}".format(x) for x in line_dict["video_feature_dim_128"]])

  return str(item_id), video_128


def read_video_features():
  video_rdd = sc.textFile(video_files)
  video_rdd = video_rdd.map(parse_video_line)
  video_rdd = video_rdd.filter(lambda x: x[0] != "-")
  video_rdd = video_rdd.reduceByKey(lambda x, y: y)
  video_rdd = video_rdd.map(lambda x: "\t".join(list(x)))
  return video_rdd

def main():
  print("========== start =======")
  video_rdd = read_video_features()
  #video_rdd.repartition(1).saveAsTextFile(output_dir)
  video_rdd.saveAsTextFile(output_dir)
  print("========== end =======")

if __name__ == '__main__':
  main()
