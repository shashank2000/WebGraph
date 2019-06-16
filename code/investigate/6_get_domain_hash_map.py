import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col, lit, regexp_replace, udf
from functools import reduce
from aut import *
import time
import os
import xxhash

import pickle

########################################################
#   Extracts domain level edges 
########################################################

#.set("spark.local.dir","/lfs/madmax4/0/dankang/tmp")\
#.set("spark.local.dir","/dfs/scratch2/dankang/tmp")\
conf = SparkConf()\
        .setMaster("local[40]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
WB_PATH = "/dfs/dataset/wb"
SEED = 0

##########################################################
##### vvvvvvv CHANGE AS NEEDED vvvvvvvvvvvv  ########
input_file_name = "domain_links_with_hash_and_counts.parquet"
output_file_name = "domain_to_hash.tsv"
file_dir = "/dfs/scratch2/dankang/wb_links/2004/"

#testing
#input_file_name = "2003_domain_links_with_hash_and_counts.parquet"
#output_file_name = "2003_domain_to_hash.tsv"
#file_dir="/dfs/scratch2/dankang/WebGraph/data/"

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

filenum = len(os.listdir(input_file_path))
print("There are {} files".format(filenum))

links = sqlContext.read.parquet(input_file_path)\
        .select("Src_domain", "Dest_domain", "Src_domain_hash", "Dest_domain_hash")

links.where(links.Src_domain_hash == '871c7b1303b71b1')\
        .show()

sys.exit()


a = links.rdd.flatMap(lambda l: [(l[2],l[0]),(l[3],l[1])])\
    .groupByKey()\
    .map(lambda l: "{}\t{}".format(l[0], ",".join(set(l[1]))))\
    .coalesce(1)\
    .saveAsTextFile(output_file_path)

all_end = time.time()
timer("Total", all_start, all_end)
