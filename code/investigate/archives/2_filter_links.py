import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import trim, col, lit, regexp_replace
from functools import reduce
from aut import *
import time
import os

import pickle

conf = SparkConf()\
        .setMaster("local[16]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
WB_PATH = "/dfs/dataset/wb"
##########################################################

##### vvvvvvv CHANGE AS NEEDED vvvvvvvvvvvv  ########
input_file_name = "general_links.parquet"
output_file_name = "links_only"
file_dir = "/dfs/scratch2/dankang/wb_links/2004/"

# If true, we only take links where both From and To portion of row
# are links. i.e. no mailTo:, sftp:, etc
ONLY_URLS = True


# FOR TESTING
#file_dir = "/dfs/scratch2/dankang/WebGraph/data/"
#input_file_name = "2003_sample_links.parquet"
#output_file_name = "2003_links_only"

input_file_path = file_dir + input_file_name
output_file_path = file_dir + output_file_name


##### ^^^^^^^ CHANGE AS NEEDED ^^^^^^^^^^^^  ########

def timer(task, start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    time_text = "[{}]{:0>2}:{:0>2}:{:05.2f}\n".format(task, int(hours),int(minutes),seconds)
    print(time_text)

all_start = time.time();
print("=" * 50)
print("Read in parquet file")

filenum = len(os.listdir(input_file_path))
print("There are {} files".format(filenum))

links = sqlContext.read.parquet(input_file_path).select("Src","Dest")
#print(links.count())

# remove non-valid links
if ONLY_URLS:
    VALID_STARTS = ["http", "www"]
    def starts_with(col_name):
        return reduce(lambda x, y: x | y, [(col(col_name).startswith(s)) for s in VALID_STARTS], lit(False))
    links = links.where(starts_with("Src")).where(starts_with("Dest"))

# format links appropriately
links = links.withColumn("Src", regexp_replace(col("Src"), "\\s+", ""))\
            .withColumn("Dest", regexp_replace(col("Dest"), "\\s+",""))\
            
            .dropDuplicates()
#print(links.count())

# save the links
# It would be ideal to output to parquet format, but that involves
# installation of snappy or lzo locally, which requires admin privileges
# for now just going on with csv format - this probably takes up 2x more storage

#links.coalesce(filenum).write.parquet(output_file_path)
#links.coalesce(filenum).write.option("encoding", "UTF-8").csv(output_file_path)
links.coalesce(filenum)\
    .write.option("encoding", "UTF-8")\
    .csv(output_file_path, sep="\t")
#links.coalesce(filenum).write.option("compression", "lzo").parquet(output_file_path)

all_end = time.time();
timer("total", all_start, all_end)

