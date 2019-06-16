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

###############################################
# Note: this code is 2nd part of 2_filter_links_index_nodes
#   - Part 2 loads results of part 1, dedup and save the hash values  
#
################################################

        #.set("spark.local.dir","/dfs/scratch2/dankang/tmp")\
        #.setMaster("local[40]")
conf = SparkConf()\
        .set("spark.local.dir","/lfs/madmax4/0/dankang/tmp")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
WB_PATH = "/dfs/dataset/wb"
SEED = 0

##########################################################
##### vvvvvvv CHANGE AS NEEDED vvvvvvvvvvvv  ########
output_hash_file_name = "filtered_links_with_hash_no_dedup.parquet"
output_file_name = "hash_links.tsv"
file_dir = "/dfs/scratch2/dankang/wb_links/2003/"

# If true, we only take links where both From and To portion of row
# are links. i.e. no mailTo:, sftp:, etc
ONLY_URLS = True

# FOR TESTING
#file_dir = "/dfs/scratch2/dankang/WebGraph/data/"
#input_file_name = "2003_filtered_links_with_hash_no_dedup.parquet"
#output_file_name = "2003_hash_links_num.tsv"

output_hash_file_path = file_dir + output_hash_file_name
output_file_path = file_dir + output_file_name

##### ^^^^^^^ CHANGE AS NEEDED ^^^^^^^^^^^^  ########
##########################################################

###=========  Helper Functions  ===========  ########
# hashing function
def xxhash_func(data):
    try:
        val =  xxhash.xxh64(data, seed=SEED).hexdigest()
    except :
        print("DKERRORRR")
        print(data)    
    return val

xxhash_udf = udf(xxhash_func, StringType())

def timer(task, start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    time_text = "[{}]{:0>2}:{:0>2}:{:05.2f}\n".format(task, int(hours),int(minutes),seconds)
    print(time_text)

def announce_task(task):
    print("=" * 50)
    print(task)


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

def normalize_url_func(url):
    url = remove_prefix(url, "http")
    url = remove_prefix(url, "https")
    url = remove_prefix(url, ":")
    url = remove_prefix(url, "//")
    url = remove_prefix(url, "/")
    url = remove_prefix(url, "www.")
    return url

normalize_url = udf(normalize_url_func, StringType())

###===========================================#######

all_start = time.time()
announce_task("Read in parquet file")

filenum = len(os.listdir(output_hash_file_path))
print("There are {} files".format(filenum))

link_start = time.time()
timer("Loading base parquet file", all_start, link_start)

announce_task("Save hashed links only")
#links.select("Src_hash","Dest_hash")\
hashed_links = sqlContext.read.parquet(output_hash_file_path).select("Src_hash","Dest_hash")
hashed_links\
    .dropDuplicates()\
    .write.option("encoding", "UTF-8")\
    .csv(output_file_path, sep="\t")

all_end = time.time()
timer("Saving hashed links only", link_start, all_end)
timer("Total", all_start, all_end)
