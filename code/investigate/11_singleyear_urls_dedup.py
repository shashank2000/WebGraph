import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from aut import *
import time

conf = SparkConf()

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
WB_PATH = "/dfs/dataset/wb"
print(sc.getConf().getAll())

def timer(task, start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    time_text = "[{}]{:0>2}:{:0>2}:{:05.2f}\n".format(task, int(hours),int(minutes),seconds)
    print(time_text)

##########################################################

year = 2003
input_path = "/dfs/scratch2/dankang/wb_links/{}_filtered/urls".format(year)
target_path = "/dfs/scratch2/dankang/wb_links/{}_filtered/urls_dedup".format(year)

#input_path = "/dfs/scratch2/dankang/WebGraph/data/2003_sample"
#target_path = "/dfs/scratch2/dankang/WebGraph/data/2003_sample_urls"
input_file_path = input_path + ".parquet"
target_file_path = target_path + ".parquet"
##########################################################

start = time.time()
print("Reading from : " + input_path)

sqlContext.read.parquet(input_file_path)\
        .dropDuplicates()\
        .write.parquet(target_file_path)

end = time.time()
timer("Total", start, end)
