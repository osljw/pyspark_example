import sys 
import os
import yaml
import argparse
import json
import pandas as pd

sys.path.append('.')
print("cur dir files:", os.listdir('.'))

from compiler.ast import flatten
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
print("pyspark location:", f.__file__)
from pyspark.sql.functions import pandas_udf, PandasUDFType


parser = argparse.ArgumentParser()
parser.add_argument(
    '--data_files', required=True, type=str, help='')
parser.add_argument(
    '--uid_files', required=True, type=str, help='')
parser.add_argument(
    '--item_id_files', required=True, type=str, help='')
parser.add_argument(
    '--author_id_files', required=True, type=str, help='')
parser.add_argument(
    '--item_city_files', required=True, type=str, help='')
parser.add_argument(
    '--output_dir', required=True, type=str, help='')
args = parser.parse_args()

data_files = args.data_files
uid_files = args.uid_files
item_id_files = args.item_id_files
author_id_files = args.author_id_files
item_city_files = args.item_city_files
output_dir = args.output_dir

spark = SparkSession\
    .builder\
    .appName("StatLocationPctr")\
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def read_data_df(input_files):
  input_schema = StructType()
  input_schema.add(StructField("instance_id", LongType(), True))
  input_schema.add(StructField("uid", LongType(), True))
  input_schema.add(StructField("user_city", StringType(), True))
  input_schema.add(StructField("item_id", StringType(), True))
  input_schema.add(StructField("author_id", StringType(), True))
  input_schema.add(StructField("item_city", StringType(), True))
  input_schema.add(StructField("channel", StringType(), True))
  input_schema.add(StructField("finish", LongType(), True))
  input_schema.add(StructField("like", LongType(), True))
  input_schema.add(StructField("music_id", StringType(), True))
  input_schema.add(StructField("device", StringType(), True))
  input_schema.add(StructField("time", LongType(), True))
  input_schema.add(StructField("duration_time", StringType(), True))
  input_schema.add(StructField("day", StringType(), True))
  input_schema.add(StructField("week", StringType(), True))
  input_schema.add(StructField("hour", StringType(), True))
  input_schema.add(StructField("words", StringType(), True))
  input_schema.add(StructField("freqs", StringType(), True))
  input_schema.add(StructField("gender", StringType(), True))
  input_schema.add(StructField("beauty", StringType(), True))
  input_schema.add(StructField("pos0", StringType(), True))
  input_schema.add(StructField("pos1", StringType(), True))
  input_schema.add(StructField("pos2", StringType(), True))
  input_schema.add(StructField("pos3", StringType(), True))
  input_schema.add(StructField("video_vec", StringType(), True))
  input_schema.add(StructField("audio_vec", StringType(), True))

  data_df = spark.read.csv(input_files, schema=input_schema, sep='\t')
  return data_df

def read_uid_df(input_files):
  uid_schema = StructType()
  uid_schema.add(StructField("uid", LongType(), True))
  uid_schema.add(StructField("uid_finish_pv", FloatType(), True))
  uid_schema.add(StructField("uid_finish_clk", FloatType(), True))
  uid_schema.add(StructField("uid_finish_ctr", FloatType(), True))
  uid_schema.add(StructField("uid_like_pv", FloatType(), True))
  uid_schema.add(StructField("uid_like_clk", FloatType(), True))
  uid_schema.add(StructField("uid_like_ctr", FloatType(), True))
  
  uid_df = spark.read.csv(input_files, schema=uid_schema, sep='\t')
  return uid_df

def read_group_df(input_files, group_name):
  uid_schema = StructType()
  uid_schema.add(StructField(group_name, LongType(), True))
  uid_schema.add(StructField(group_name+"_finish_pv", FloatType(), True))
  uid_schema.add(StructField(group_name+"_finish_clk", FloatType(), True))
  uid_schema.add(StructField(group_name+"_finish_ctr", FloatType(), True))
  uid_schema.add(StructField(group_name+"_like_pv", FloatType(), True))
  uid_schema.add(StructField(group_name+"_like_clk", FloatType(), True))
  uid_schema.add(StructField(group_name+"_like_ctr", FloatType(), True))
  
  uid_df = spark.read.csv(input_files, schema=uid_schema, sep='\t')
  return uid_df

def main():
  data_df = read_data_df(data_files)
  uid_df = read_uid_df(uid_files)
  item_id_df = read_group_df(item_id_files, "item_id")
  author_id_df = read_group_df(author_id_files, "author_id")
  item_city_df = read_group_df(item_city_files, "item_city")

  # join data
  data_df = data_df.join(uid_df, on=["uid"], how="left_outer")
  data_df = data_df.join(item_id_df, on=["item_id"], how="left_outer")
  data_df = data_df.join(author_id_df, on=["author_id"], how="left_outer")
  data_df = data_df.join(item_city_df, on=["item_city"], how="left_outer")

  data_columns = ["uid",
          "item_id", "author_id", "item_city", "channel",
          "finish", "like",
          "music_id", "device", "time", "duration_time",
          "day", "week", "hour",
          "words", "freqs",
          "gender", "beauty",
          "pos0", "pos1", "pos2", "pos3",
          "video_vec", "audio_vec",
          "uid_finish_pv", "uid_finish_clk", "uid_finish_ctr",
          "uid_like_pv", "uid_like_clk", "uid_like_ctr",
          "item_id_finish_pv", "item_id_finish_clk", "item_id_finish_ctr",
          "item_id_like_pv", "item_id_like_clk", "item_id_like_ctr",
          "author_id_finish_pv", "author_id_finish_clk", "author_id_finish_ctr",
          "author_id_like_pv", "author_id_like_clk", "author_id_like_ctr",
          "item_city_finish_pv", "item_city_finish_clk", "item_city_finish_ctr",
          "item_city_like_pv", "item_city_like_clk", "item_city_like_ctr",
          ]

  data_df = data_df[data_columns]
  print("train column order:", data_df.schema)
  data_df.write.save(output_dir, format="csv", sep='\t')

if __name__ == '__main__':
  print("========== start =======")
  main()
  print("========== end =======")
