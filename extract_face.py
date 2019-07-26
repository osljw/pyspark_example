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
    '--face_files', required=True, type=str, help='')
parser.add_argument(
    '--output_dir', required=True, type=str, help='')
args = parser.parse_args()

face_files = args.face_files
output_dir = args.output_dir

spark = SparkSession\
    .builder\
    .appName("StatLocationPctr")\
    .getOrCreate()
sc = spark.sparkContext

def parse_face_line(line):
  line = line.strip()
  line_dict = json.loads(line)
  if len(line_dict["face_attrs"]) == 0:
      return "-", "-"
  item_id = line_dict["item_id"]
  genders_list = [face_attr["gender"] for face_attr in line_dict["face_attrs"]]
  beautys_list = [face_attr["beauty"] for face_attr in line_dict["face_attrs"]]
  gender_and_beauty = zip(genders_list, beautys_list)
  gender0_beauty_list = [y for x, y in gender_and_beauty if x == 0]
  gender1_beauty_list = [y for x, y in gender_and_beauty if x == 1]
  face_pos0s = [face_attr["relative_position"][0] for face_attr in line_dict["face_attrs"]]
  face_pos1s = [face_attr["relative_position"][1] for face_attr in line_dict["face_attrs"]]
  face_pos2s = [face_attr["relative_position"][2] for face_attr in line_dict["face_attrs"]]
  face_pos3s = [face_attr["relative_position"][3] for face_attr in line_dict["face_attrs"]]

  # face count
  face_count = str(len(genders_list))
  face_gender0_count = str(genders_list.count(0))
  face_gender1_count = str(genders_list.count(1))

  # face beauty
  face_beauty_max = max(beautys_list)
  face_beauty_min = min(beautys_list)
  face_beauty_diff = str(face_beauty_max - face_beauty_min)
  face_gender0_beauty_max = str(max(gender0_beauty_list)) if gender0_beauty_list else '-1'
  face_gender0_beauty_min = str(min(gender0_beauty_list)) if gender0_beauty_list else '-1'
  face_gender1_beauty_max = str(max(gender1_beauty_list)) if gender1_beauty_list else '-1'
  face_gender1_beauty_min = str(min(gender1_beauty_list)) if gender1_beauty_list else '-1'
  
  # face area
  face_area = zip(face_pos2s, face_pos3s)
  face_area_list = [x*y for x, y in face_area]
  face_gender_area_list = zip(genders_list, face_area_list)
  face_gender0_area_list = [y for x, y in face_gender_area_list if x == 0]
  face_gender1_area_list = [y for x, y in face_gender_area_list if x == 1]

  face_area_max = str(max(face_area_list))
  face_area_min = str(min(face_area_list))
  face_area_total = str(sum(face_area_list))
  face_gender0_area_max = str(max(face_gender0_area_list)) if face_gender0_area_list else '-1' 
  face_gender0_area_min = str(min(face_gender0_area_list)) if face_gender0_area_list else '-1' 
  face_gender0_area_total = str(sum(face_gender0_area_list)) if face_gender0_area_list else '-1'
  face_gender1_area_max = str(max(face_gender1_area_list)) if face_gender1_area_list else '-1' 
  face_gender1_area_min = str(min(face_gender1_area_list)) if face_gender1_area_list else '-1' 
  face_gender1_area_total = str(sum(face_gender1_area_list)) if face_gender1_area_list else '-1'


  #face_genders = ",".join([str(x) for x in genders_list])
  #face_beautys = ",".join([str(x) for x in beautys_list])

  value_list = [
      face_count,
      face_gender0_count,
      face_gender1_count,

      face_beauty_max,
      face_beauty_min,
      face_beauty_diff,
      face_gender0_beauty_max,
      face_gender0_beauty_min,
      face_gender1_beauty_max,
      face_gender1_beauty_min,

      face_area_max,
      face_area_min,
      face_area_total,
      face_gender0_area_max,
      face_gender0_area_min,
      face_gender0_area_total,
      face_gender1_area_max,
      face_gender1_area_min,
      face_gender1_area_total,
      ]

  item_id = str(item_id)
  value_list = "\t".join([str(x) for x in value_list])
  return item_id, value_list

def read_face_features():
  face_rdd = sc.textFile(face_files)
  face_rdd = face_rdd.map(parse_face_line)
  face_rdd = face_rdd.filter(lambda x: x[0] != "-") #filter null feature
  face_rdd = face_rdd.reduceByKey(lambda x, y: y) #unique
  face_rdd = face_rdd.map(lambda x: "\t".join(list(x)))
  return face_rdd

def main():
  print("========== start =======")
  face_rdd = read_face_features()
  #face_rdd.repartition(1).saveAsTextFile(output_dir)
  face_rdd.saveAsTextFile(output_dir)
  print("========== end =======")

if __name__ == '__main__':
  main()
