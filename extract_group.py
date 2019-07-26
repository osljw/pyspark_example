from __future__ import division,print_function
import sys 
import os
import yaml
import argparse
import json
import pandas as pd

sys.path.append('.')
print("cur dir files:", os.listdir('.'))

from compiler.ast import flatten
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import *
import pyspark.sql.functions as f
print("pyspark location:", f.__file__)
from pyspark.sql.functions import pandas_udf, PandasUDFType


parser = argparse.ArgumentParser()
parser.add_argument(
    '--data_files', required=True, type=str, help='')
parser.add_argument(
    '--group_name', required=True, type=str, help='')
parser.add_argument(
    '--output_dir', required=True, type=str, help='')
args = parser.parse_args()

data_files = args.data_files
group_name = args.group_name
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

def min_max(df, column_names):
  min_funcs = [f.min(x) for x in column_names]
  max_funcs = [f.max(x) for x in column_names]
  column_funcs = min_funcs + max_funcs
  #min_value, max_value = df.select(f.min(column_name), f.max(column_name)).first()
  min_max_value = df.select(*column_funcs).first()
  print("min_max_value:", min_max_value)
  for i, column_name in enumerate(column_names):
    min_value = min_max_value[i]
    max_value = min_max_value[i + len(column_names)]
    if min_value == max_value:
        print("__error__: column_name:{} min_value == max_value".format(column_name))
        continue
    df = df.withColumn(column_name, f.log1p(column_name))
    df = df.withColumn(column_name, f.round((f.col(column_name) - min_value)/(max_value - min_value), 6))
  return df

def min_max2(df, column_names):
  for i, column_name in enumerate(column_names):
    rdd = df.rdd.map(lambda row: row[column_name])
    min_value = rdd.min()
    max_value = rdd.max()
    print("column:", column_name, "min_max_value:", min_value, max_value)
    if min_value == max_value:
        print("__error__: column_name:{} min_value == max_value".format(column_name))
        continue
    df = df.withColumn(column_name, f.round((f.col(column_name) - min_value)/(max_value - min_value), 6))
  return df


def build_group_df2(data_df, group_name):
  out_schema = StructType()
  out_schema.add(StructField(group_name, StringType(), True))
  out_schema.add(StructField(group_name + "_finish_pv", LongType(), True))
  out_schema.add(StructField(group_name + "_finish_clk", LongType(), True))
  out_schema.add(StructField(group_name + "_finish_ctr", FloatType(), True))
  out_schema.add(StructField(group_name + "_like_pv", LongType(), True))
  out_schema.add(StructField(group_name + "_like_clk", LongType(), True))
  out_schema.add(StructField(group_name + "_like_ctr", FloatType(), True))
  print("group:", group_name, "schema:", out_schema)
  @f.pandas_udf(out_schema, f.PandasUDFType.GROUPED_MAP)
  def extract_func(pdf):
    d = {}
    d[group_name] = [pdf[group_name][0]]
    d[group_name + '_finish_pv'] =  [len(pdf['finish'])]
    d[group_name + '_finish_clk'] = [sum(pdf['finish'])]
    d[group_name + '_finish_ctr'] = [d[group_name+'_finish_clk'][0] / d[group_name+'_finish_pv'][0]]
    d[group_name + '_like_pv'] =  [len(pdf['like'])]
    d[group_name + '_like_clk'] = [sum(pdf['like'])]
    d[group_name + '_like_ctr'] = [d[group_name+'_like_clk'][0] / d[group_name+'_like_pv'][0]]
    df = pd.DataFrame(d, columns=out_schema.fieldNames())
    return df
  print("before gourp data_df:", data_df.schema)
  #print("before group data_df take:", data_df.take(3))
  group_df = data_df.groupby([group_name]).apply(extract_func)
  print("group_df:", group_df.schema)
  #print("group_df take:", group_df.collect())

  # minmax to range [0,1] 
  min_max_columns = [group_name+'_finish_pv', 
          group_name + '_finish_clk', 
          group_name + '_like_pv', 
          group_name + '_like_clk']
  print("min_max_columns:", min_max_columns)
  group_df = min_max(group_df, min_max_columns)

  return group_df

def build_group_df(data_df, group_name):
  group_df = data_df.groupby([group_name]).agg(
          f.count('finish').alias(group_name+'_finish_pv'),
          f.sum('finish').alias(group_name+'_finish_clk'),
          f.avg('finish').alias(group_name+'_finish_ctr'),
          f.count('like').alias(group_name+'_like_pv'),
          f.sum('like').alias(group_name+'_like_clk'),
          f.avg('like').alias(group_name+'_like_ctr'),
          )
  min_max_columns = [group_name+'_finish_pv', 
          group_name + '_finish_clk', 
          group_name + '_like_pv', 
          group_name + '_like_clk']
  group_df = min_max(group_df, min_max_columns)
  return group_df

def build_group_df3(data_df, group_name):
  def seqOp(x, y):
      print("seq x", x)
      return x+y
  def combOp(x, y):
      print("comb x", x)
      return x+y
  rdd = data_df.rdd.map(lambda row: (row['item_city'], row['like']))
  print("rdd take:", rdd.take(3))
  rdd = rdd.aggregateByKey(0, seqOp, combOp)
  print("rdd aggregate:", rdd.collect())

  #input_schema = StructType()
  #input_schema.add(StructField("feature", LongType(), True))
  #df = sqlContext.createDataFrame(rdd, input_schema)
  return rdd


def main():
  data_df = read_data_df(data_files)
  #print("====data_df take:", data_df.take(3))
  group_df = build_group_df(data_df, group_name)
  #sys.exit()

  print("group:", group_name, "column order:", group_df.schema)
  group_df.write.save(output_dir, format="csv", sep='\t')

if __name__ == '__main__':
  print("========== start =======")
  main()
  print("========== end =======")
