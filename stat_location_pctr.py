#!/usr/bin/python
#coding=utf-8

#from pyspark import SparkContext, SparkConf
#conf = SparkConf().setAppName(appName).setMaster(master)
#sc = SparkContext(conf=conf)
#sc.textFile() # rdd

import sys
import os
import yaml
import argparse
import json
from compiler.ast import flatten

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

parser = argparse.ArgumentParser()
parser.add_argument(
    '--in_file', required=True, type=str, default='',
        help='')
parser.add_argument(
    '--rankserver_log_dir', required=True, type=str, default='',
        help='')
parser.add_argument(
    '--out_dir', required=True, type=str, default='',
        help='')
args = parser.parse_args()

in_file = args.in_file
rankserver_log_dir = args.rankserver_log_dir
out_dir = args.out_dir

print(os.listdir('.'))
print(os.listdir('conf'))

spark = SparkSession\
    .builder\
    .appName("StatLocationPctr")\
    .getOrCreate()
sc = spark.sparkContext

def load_schema(file_name):
    column_names = []
    with open(file_name) as fd:
        for line in fd:
            if not line.strip() or line[0] == '#':
                continue
            index, _column_name = line.strip().split(':')
            column_names.append(_column_name.strip())
    return column_names

def load_feature(file_name):
    with open(file_name) as fd:
        feature_info = yaml.load(fd)
    return feature_info

def get_used_feature(feature_info, cross_feature_info):
    used_feature = set(feature_info.keys())
    used_cross_feature = []
    for feats in cross_feature_info.keys():
        used_cross_feature += feats.split('&')
    used_cross_feature = set(used_cross_feature)
    return list(used_feature | used_cross_feature)


column_names = load_schema('conf/schema.yaml')
input_schema = StructType()
for col in column_names:
    input_schema.add(StructField(col, StringType(), True))
# or use schema = spark.read.schema("col0 INT, col2 DOUBLE")

def map_log_data(line, filter_site, filter_category):
    line = line.strip()
    if line.find("QUERY_RESULT") < 0:
        return "-"
    ss = json.loads(line)["body"].strip().split(" ")
    dic_info = {}
    need_fields = ["rid", "platform", "category", "location", "cast_way"]
    for s in ss:
        info = s.strip().split(":")
        if len(info) != 2:
            continue
        name, value = info
        if name not in need_fields:
            continue
        dic_info[name] = value

    if len(dic_info) != len(need_fields):
        return "-"
    if dic_info["platform"] != filter_site or dic_info["category"] != filter_category:
        return "-"
    if dic_info["cast_way"] != "1":
        return "-"
    return dic_info["rid"], dic_info["location"]

def read_log_df():
    site = "1"
    category = "FOCUS2"
    log_rdd = sc.textFile(rankserver_log_dir)
    log_rdd = log_rdd.map(lambda x: map_log_data(x, site, category))
    log_rdd = log_rdd.filter(lambda x: x != "-")
    log_df = spark.createDataFrame(log_rdd, schema=["request_id", "location"])
    return log_df

def read_raw_df():
    df = spark.read.csv(in_file, schema=input_schema, sep='\t')
    df = df[['request_id', 'clk', 'site', 'category', 'location', 'ctr']]
    df = df.filter(
            (f.col('site') == '1') & 
            (f.col('category') == 'FOCUS2')
            )
    df = df.withColumn('ctr', f.col('ctr')/1000000)
    return df

def main():
    print("=========== stat start =============")

    raw_df = read_raw_df()
    log_df = read_log_df()
    raw_df.printSchema()
    log_df.printSchema()
    df = raw_df.join(log_df, ["request_id", "location"])

    #print("="*40)
    #print(df.take(2))
    df = df.groupBy(['location']).agg(
            f.count('clk'),
            f.sum('clk'), 
            f.sum('ctr'), 
            f.sum('ctr')/f.sum('clk'))
    df.repartition(1).write.csv(path=out_dir, header=False)

if __name__ == '__main__':
    main()
