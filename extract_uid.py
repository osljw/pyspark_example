from __future__ import division
import sys 
import os
import yaml
import argparse
import json
import numpy as np
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
    '--output_dir', required=True, type=str, help='')
args = parser.parse_args()

data_files = args.data_files
output_dir = args.output_dir

spark = SparkSession\
    .builder\
    .appName("StatLocationPctr")\
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def read_data_df(input_files):
  face_column_names = [
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

  input_schema = StructType()
  input_schema.add(StructField("instance_id", StringType(), True))
  input_schema.add(StructField("uid", StringType(), True))
  input_schema.add(StructField("user_city", StringType(), True))
  input_schema.add(StructField("item_id", StringType(), True))
  input_schema.add(StructField("author_id", StringType(), True))
  input_schema.add(StructField("item_city", StringType(), True))
  input_schema.add(StructField("channel", StringType(), True))
  input_schema.add(StructField("finish", StringType(), True))
  input_schema.add(StructField("like", StringType(), True))
  input_schema.add(StructField("music_id", StringType(), True))
  input_schema.add(StructField("device", StringType(), True))
  input_schema.add(StructField("time", StringType(), True))
  input_schema.add(StructField("duration_time", StringType(), True))
  input_schema.add(StructField("day", StringType(), True))
  input_schema.add(StructField("week", StringType(), True))
  input_schema.add(StructField("hour", StringType(), True))
  input_schema.add(StructField("words", StringType(), True))
  input_schema.add(StructField("freqs", StringType(), True))
  #input_schema.add(StructField("gender", StringType(), True))
  #input_schema.add(StructField("beauty", StringType(), True))
  #input_schema.add(StructField("pos0", StringType(), True))
  #input_schema.add(StructField("pos1", StringType(), True))
  #input_schema.add(StructField("pos2", StringType(), True))
  #input_schema.add(StructField("pos3", StringType(), True))
  input_schema.add(StructField("video_vec", StringType(), True))
  input_schema.add(StructField("audio_vec", StringType(), True))

  for column_name in face_column_names:
    input_schema.add(StructField(column_name, StringType(), True))

  data_df = spark.read.csv(input_files, schema=input_schema, sep='\t')
  return data_df

def min_max(df, column_names):
  min_funcs = [f.min(x) for x in column_names]
  max_funcs = [f.max(x) for x in column_names]
  column_funcs = min_funcs + max_funcs
  #min_value, max_value = df.select(f.min(column_name), f.max(column_name)).first()
  min_max_value = df.select(*column_funcs).first()
  for i, column_name in enumerate(column_names):
    min_value = min_max_value[i]
    max_value = min_max_value[i + len(column_names)]
    if min_value == max_value:
        print("__error__: column_name:{} min_value == max_value".format(column_name))
        continue
    df = df.withColumn(column_name, f.round((f.col(column_name) - min_value)/(max_value - min_value), 6))
  return df


def build_uid_df(data_df):
  uid_schema = StructType()
  uid_schema.add(StructField("uid", StringType(), True))
  uid_schema.add(StructField("item_id", StringType(), True))
  uid_schema.add(StructField("author_id", StringType(), True))
  uid_schema.add(StructField("channel", StringType(), True))
  uid_schema.add(StructField("music_id", StringType(), True))
  uid_schema.add(StructField("device", StringType(), True))
  uid_schema.add(StructField("duration_time", StringType(), True))

  uid_schema.add(StructField("item_id_looked_list", StringType(), True))
  uid_schema.add(StructField("author_id_looked_list", StringType(), True))
  uid_schema.add(StructField("channel_looked_list", StringType(), True))
  uid_schema.add(StructField("music_id_looked_list", StringType(), True))

  uid_schema.add(StructField("hour_uniq_looked_list", StringType(), True))
  uid_schema.add(StructField("author_id_uniq_looked_list", StringType(), True))
  uid_schema.add(StructField("channel_uniq_looked_list", StringType(), True))
  uid_schema.add(StructField("music_id_uniq_looked_list", StringType(), True))
  
  uid_schema.add(StructField("face_count_cumsum", StringType(), True))
  uid_schema.add(StructField("face_gender0_count_cumsum", StringType(), True))
  uid_schema.add(StructField("face_gender1_count_cumsum", StringType(), True))

  print("uid schema:", uid_schema)

  @f.pandas_udf(uid_schema, f.PandasUDFType.GROUPED_MAP)
  def uid_extract(idf):
    idf = idf.sort_values('time')
    
    uid_list = []
    item_id_list = []
    author_id_list = []
    channel_list = []
    music_id_list = []
    device_list = []
    duration_time_list = []
    hour_list = []

    item_id_looked_list = []
    author_id_looked_list = []
    channel_looked_list = []
    music_id_looked_list = []

    hour_uniq_looked_list = []
    author_id_uniq_looked_list = []
    channel_uniq_looked_list = []
    music_id_uniq_looked_list = []

    face_count_cumsum = 0
    face_gender0_count_cumsum = 0
    face_gender1_count_cumsum = 0
    face_count_cumsum_list = []
    face_gender0_count_cumsum_list = []
    face_gender1_count_cumsum_list = []

    for index, row in idf.iterrows():
        uid = str(row['uid'])
        item_id = str(row['item_id'])
        author_id = str(row['author_id'])
        channel = str(row['channel'])
        music_id = str(row['music_id'])
        device = str(row['device'])
        duration_time = str(row['duration_time'])
        hour = str(row['hour'])
        face_count = str(row['face_count'])  if row['face_count'] else '0'
        face_gender0_count = str(row['face_gender0_count']) if row['face_gender0_count'] else '0'
        face_gender1_count = str(row['face_gender1_count']) if row['face_gender1_count'] else '0'

        # looked num count
        item_id_look = item_id_list.count(item_id) 
        author_id_look = author_id_list.count(author_id) 
        channel_look = channel_list.count(channel) 
        music_id_look = music_id_list.count(music_id)

        item_id_looked_list.append(str(item_id_look))
        author_id_looked_list.append(str(author_id_look))
        channel_looked_list.append(str(channel_look))
        music_id_looked_list.append(str(music_id_look))

        # face count
        face_count_cumsum += int(face_count)
        face_gender0_count_cumsum += int(face_gender0_count)
        face_gender1_count_cumsum += int(face_gender1_count)

        face_count_cumsum_list.append(str(face_count_cumsum))
        face_gender0_count_cumsum_list.append(str(face_gender0_count_cumsum))
        face_gender1_count_cumsum_list.append(str(face_gender1_count_cumsum))

        # origin feature
        uid_list.append(uid)
        item_id_list.append(item_id)
        author_id_list.append(author_id)
        channel_list.append(channel)
        music_id_list.append(music_id)
        device_list.append(device)
        duration_time_list.append(duration_time)
        hour_list.append(hour)
        
        # uniq stat feature
        hour_uniq_look = len(set(hour_list))
        author_id_uniq_look = len(set(author_id_list))
        channel_uniq_look = len(set(channel_list))
        music_id_uniq_look = len(set(music_id_list))

        hour_uniq_looked_list.append(str(hour_uniq_look))
        author_id_uniq_looked_list.append(str(author_id_uniq_look))
        channel_uniq_looked_list.append(str(channel_uniq_look))
        music_id_uniq_looked_list.append(str(music_id_uniq_look))

    odf = pd.DataFrame({
        'uid':uid_list,
        'item_id':item_id_list,
        'author_id':author_id_list,
        'channel':channel_list,
        'music_id':music_id_list,
        'device':device_list,
        'duration_time':duration_time_list,

        'item_id_looked_list':item_id_looked_list,
        'author_id_looked_list':author_id_looked_list,
        'channel_looked_list':channel_looked_list,
        'music_id_looked_list':music_id_looked_list,

        'hour_uniq_looked_list':hour_uniq_looked_list,
        'author_id_uniq_looked_list':author_id_uniq_looked_list,
        'channel_uniq_looked_list':channel_uniq_looked_list,
        'music_id_uniq_looked_list':music_id_uniq_looked_list,

        'face_count_cumsum':face_count_cumsum_list,
        'face_gender0_count_cumsum':face_gender0_count_cumsum_list,
        'face_gender1_count_cumsum':face_gender1_count_cumsum_list,
        })

    print("odf:", odf)
    return odf

  uid_df = data_df.groupby(['uid']).apply(uid_extract)
  #print("uid_df:", uid_df.take(3))

  # minmax to range [0,1] 
  #min_max_columns = ['uid_finish_pv', 'uid_finish_clk', 'uid_like_pv', 'uid_like_clk']
  #uid_df = min_max(uid_df, min_max_columns)

  return uid_df


def main():
  data_df = read_data_df(data_files)
  uid_df = build_uid_df(data_df)
  #print("uid df:", uid_df.take(10))
  #sys.exit()

  #data_df = data_df[data_columns]
  print("uid_df column order:", uid_df.schema)
  uid_df.write.save(output_dir, format="csv", sep='\t')

if __name__ == '__main__':
  print("========== start =======")
  main()
  print("========== end =======")
