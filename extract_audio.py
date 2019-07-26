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
    '--audio_files', required=True, type=str, help='')
parser.add_argument(
    '--output_dir', required=True, type=str, help='')
args = parser.parse_args()

audio_files = args.audio_files
output_dir = args.output_dir

spark = SparkSession\
    .builder\
    .appName("StatLocationPctr")\
    .getOrCreate()
sc = spark.sparkContext

def parse_audio_line(line):
  line = line.strip()
  line_dict = json.loads(line)
  if len(line_dict["audio_feature_128_dim"]) == 0:
      return "-","-"
  item_id = line_dict["item_id"]
  #audio_128 = ",".join([str(x) for x in line_dict["audio_feature_128_dim"]])
  audio_128 = ",".join(["{:.6f}".format(x) for x in line_dict["audio_feature_128_dim"]])

  return str(item_id), audio_128


def read_audio_features():
  audio_rdd = sc.textFile(audio_files)
  audio_rdd = audio_rdd.map(parse_audio_line)
  audio_rdd = audio_rdd.filter(lambda x: x[0] != "-")
  audio_rdd = audio_rdd.reduceByKey(lambda x, y: y)
  audio_rdd = audio_rdd.map(lambda x: "\t".join(list(x)))
  return audio_rdd

def main():
  print("========== start =======")
  audio_rdd = read_audio_features()
  #audio_rdd.repartition(1).saveAsTextFile(output_dir)
  audio_rdd.saveAsTextFile(output_dir)
  print("========== end =======")

if __name__ == '__main__':
  main()
