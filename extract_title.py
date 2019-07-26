import sys 
import os import yaml
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f


parser = argparse.ArgumentParser()
parser.add_argument(
    '--title_files', required=True, type=str, help='')
parser.add_argument(
    '--output_dir', required=True, type=str, help='')
args = parser.parse_args()

title_files = args.title_files
output_dir = args.output_dir

spark = SparkSession\
    .builder\
    .appName("StatLocationPctr")\
    .getOrCreate()
sc = spark.sparkContext

def parse_title_line(line):
  line = line.strip()
  line_dict = json.loads(line)
  if len(line_dict["title_features"]) == 0:
    return "-", "-"

  item_id = line_dict["item_id"]
  title_words = ",".join([str(x) for x in line_dict["title_features"].keys()])
  title_freqs = ",".join([str(x) for x in line_dict["title_features"].values()])

  #return "\t".join([str(item_id), title_words, title_freqs])
  return str(item_id), "\t".join([title_words, title_freqs])


def read_title_features():
  title_rdd = sc.textFile(title_files)
  title_rdd = title_rdd.map(parse_title_line)
  title_rdd = title_rdd.filter(lambda x: x[0] != "-")
  title_rdd = title_rdd.reduceByKey(lambda x, y: y)
  title_rdd = title_rdd.map(lambda x: "\t".join(list(x)))
  
  return title_rdd

def main():
  print("========== start =======")
  title_rdd = read_title_features()
  title_rdd.repartition(1).saveAsTextFile(output_dir)
  print("========== end =======")

if __name__ == '__main__':
  main()
