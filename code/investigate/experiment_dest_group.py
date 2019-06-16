import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col, lit, regexp_replace, udf, desc
from functools import reduce
from aut import *
import time
import os
import xxhash

import pickle


###############################################
# Note: this code is 1st part of 2_filter_links_index_nodes
#   - Part 1 normalizes links and creates hash values (but not deduped)
#
################################################

#.set("spark.local.dir","/lfs/madmax4/0/dankang/tmp")\
conf = SparkConf()\
        .set("spark.local.dir","/dfs/scratch2/dankang/tmp")\
        .setMaster("local[40]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
WB_PATH = "/dfs/dataset/wb"
SEED = 0

##########################################################
##### vvvvvvv CHANGE AS NEEDED vvvvvvvvvvvv  ########
input_file_name = "filtered_links_with_hash_no_dedup.parquet"
output_file_name = "dest_group.tsv"
file_dir = "/dfs/scratch2/dankang/wb_links/2003/"

# FOR TESTING
#file_dir = "/dfs/scratch2/dankang/WebGraph/data/"
#input_file_name = "2003_sample_links.parquet"
#output_hash_file_name = "2003_filtered_links_with_hash_no_dedup.parquet"
#output_file_name = "2003_hash_links_num.tsv"

input_file_path = file_dir + input_file_name
output_file_path = file_dir + output_file_name

##### ^^^^^^^ CHANGE AS NEEDED ^^^^^^^^^^^^  ########
##########################################################

###=========  Helper Functions  ===========  ########
def timer(task, start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    time_text = "[{}]{:0>2}:{:0>2}:{:05.2f}\n".format(task, int(hours),int(minutes),seconds)
    print(time_text)

def announce_task(task):
    print("=" * 50)
    print(task)

###===========================================#######

all_start = time.time()
announce_task("Read in parquet file")

links = sqlContext.read.parquet(input_file_path).select("Dest")
links = links.groupby("Dest").count()\
    .sort(desc("count"))\

links.limit(10000)\
    .coalesce(1)\
    .write.option("encoding", "UTF-8")\
    .csv(output_file_path, sep="\t")


all_end = time.time()
timer("Total", all_start, all_end)
