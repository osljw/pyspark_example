from __future__ import division
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
    '--train_files', required=True, type=str, help='')
parser.add_argument(
    '--test_files', required=True, type=str, help='')
parser.add_argument(
    '--title_files', required=True, type=str, help='')
parser.add_argument(
    '--face_files', required=True, type=str, help='')
parser.add_argument(
    '--video_files', required=True, type=str, help='')
parser.add_argument(
    '--audio_files', required=True, type=str, help='')
parser.add_argument(
    '--train_output_dir', required=True, type=str, help='')
parser.add_argument(
    '--test_output_dir', required=True, type=str, help='')
parser.add_argument(
    '--dict_output_dir', required=True, type=str, help='')
args = parser.parse_args()

train_files = args.train_files
test_files = args.test_files
title_files = args.title_files
face_files = args.face_files
video_files = args.video_files
audio_files = args.audio_files
train_output_dir = args.train_output_dir
test_output_dir = args.test_output_dir
dict_output_dir = args.dict_output_dir

spark = SparkSession\
    .builder\
    .appName("StatLocationPctr")\
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def read_data_df(input_files):
  input_schema = StructType()
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
  input_schema.add(StructField("instance_id", LongType(), True))

  data_df = spark.read.csv(input_files, schema=input_schema, sep='\t')
  return data_df

def read_title_df(input_files):
  input_schema = StructType()
  input_schema.add(StructField("item_id", StringType(), True))
  input_schema.add(StructField("words", StringType(), True))
  input_schema.add(StructField("freqs", StringType(), True))

  title_df = spark.read.csv(input_files, schema=input_schema, sep='\t')
  return title_df

#def read_face_df(input_files):
#  input_schema = StructType()
#  input_schema.add(StructField("item_id", StringType(), True))
#  input_schema.add(StructField("gender", StringType(), True))
#  input_schema.add(StructField("beauty", StringType(), True))
#  input_schema.add(StructField("pos0", StringType(), True))
#  input_schema.add(StructField("pos1", StringType(), True))
#  input_schema.add(StructField("pos2", StringType(), True))
#  input_schema.add(StructField("pos3", StringType(), True))
#
#  face_df = spark.read.csv(input_files, schema=input_schema, sep='\t')
#  return face_df

def read_face_df(input_files, column_names):
  input_schema = StructType()
  for column_name in column_names:
    input_schema.add(StructField(column_name, StringType(), True))
  face_df = spark.read.csv(input_files, schema=input_schema, sep='\t')
  return face_df

def read_video_df(input_files):
  input_schema = StructType()
  input_schema.add(StructField("item_id", StringType(), True))
  input_schema.add(StructField("video_vec", StringType(), True))

  video_df = spark.read.csv(input_files, schema=input_schema, sep='\t')
  return video_df

def read_audio_df(input_files):
  input_schema = StructType()
  input_schema.add(StructField("item_id", StringType(), True))
  input_schema.add(StructField("audio_vec", StringType(), True))

  audio_df = spark.read.csv(input_files, schema=input_schema, sep='\t')
  return audio_df

def generate_dict(df, columns):
  def convert_row(x):
      ret = []
      for feat_name,feat_values in x.asDict().items():
          if not feat_values: continue
          for feat_value in str(feat_values).split(','):
              ret.append((feat_name, feat_value))
      return ret

  ## operate DataFrame by rdd
  # select column
  df = df[columns]
  # convert Row to tuple, and flat them
  rdd = df.rdd.flatMap(convert_row)
  # stat frequency
  rdd = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y)
  #rdd = rdd.map(lambda x: '\t'.join([str(e) for e in flatten(x)]))
  rdd = rdd.map(lambda x: flatten(x))
  print("dict rdd:", rdd)
  input_schema = StructType()
  input_schema.add(StructField("feature", StringType(), True))
  input_schema.add(StructField("value", StringType(), True))
  input_schema.add(StructField("count", StringType(), True))
  df = sqlContext.createDataFrame(rdd, input_schema)
  return df


def convert_time(data_df, min_time):
  data_df = data_df.withColumn('day', f.floor((f.col('time') - min_time) / (3600 * 24)).cast('integer'))
  data_df = data_df.withColumn('week', f.col('day') % 7)
  data_df = data_df.withColumn('hour', f.floor((f.col('time') - min_time) / 3600).cast('integer') % 24)
  #data_df = data_df.withColumn('hour', f.round(f.col('hour')).cast('integer'))
  return data_df

def main():
  face_column_names = [
   "item_id",
   "face_count",
   "face_gender0_count",
   "face_gender1_count",
   
   "face_beauty_max",
   "face_beauty_min",
   "face_beauty_diff",
   "face_gender0_beauty_max",
   "face_gender0_beauty_min",
   "face_gender1_beauty_max",
   "face_gender1_beauty_min",
   
   "face_area_max",
   "face_area_min",
   "face_area_total",
   "face_gender0_area_max",
   "face_gender0_area_min",
   "face_gender0_area_total",
   "face_gender1_area_max",
   "face_gender1_area_min",
   "face_gender1_area_total",
  ]


  data_df = read_data_df(train_files)
  test_df = read_data_df(test_files)
  title_df = read_title_df(title_files)
  face_df = read_face_df(face_files, face_column_names)
  video_df = read_video_df(video_files)
  audio_df = read_audio_df(audio_files)

  # convert time to day and hour
  min_time = data_df.select(f.min("time")).first()[0]
  #min_time = test_df.select(f.min("time")).first()[0]
  print("min_time:", min_time)

  data_df = data_df.withColumn('data_type', f.lit('train'))
  test_df = test_df.withColumn('data_type', f.lit('test'))
  data_df = data_df.union(test_df)
  data_df = convert_time(data_df, min_time)

  # join data
  data_df = data_df.join(title_df, on=["item_id"], how="left_outer")
  data_df = data_df.join(face_df, on=["item_id"], how="left_outer")
  data_df = data_df.join(video_df, on=["item_id"], how="left_outer")
  data_df = data_df.join(audio_df, on=["item_id"], how="left_outer")
  #data_df = data_df.orderBy("instance_id", ascending=True)

  # generate dict
  dict_columns = ["uid", "user_city", 
          "item_id", "author_id", "item_city", "channel",
          "music_id", "device", "duration_time",
          "day", "week", "hour",
          "words",
          ] 
  dict_df = generate_dict(data_df, dict_columns)
  print("dict_df schema:", dict_df.schema)
  dict_df = dict_df.orderBy(['feature', 'count'], ascending=[True, False])
  dict_df.write.partitionBy("feature").save(dict_output_dir, format="csv", sep='\t')
  #dict_df.repartition("feature").write.save(dict_output_dir, format="csv", sep='\t')

  data_df = data_df.withColumn('partition_day', f.col('day'))
  test_df = data_df.filter(f.col('data_type') == 'test')
  data_df = data_df.filter(f.col('data_type') == 'train')

  print("data_df schema:", data_df.schema)
  data_columns = ["partition_day", "instance_id", "uid", "user_city", 
          "item_id", "author_id", "item_city", "channel",
          "finish", "like",
          "music_id", "device", "time", "duration_time",
          "day", "week", "hour",
          "words", "freqs",
          "video_vec", "audio_vec",
          ] + face_column_names[1:]

  data_df = data_df[data_columns]
  print("train column order:", data_df.schema)
  #data_df.coalesce(1).write.save(output_dir, format="csv", sep='\t')
  data_df.write.partitionBy('partition_day').save(train_output_dir, format="csv", sep='\t')

  test_df = test_df[data_columns]
  print("test column order:", test_df.schema)
  test_df.write.partitionBy('partition_day').save(test_output_dir, format="csv", sep='\t')

if __name__ == '__main__':
  print("========== start =======")
  main()
  print("========== end =======")
