import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from aut import *
import time

conf = SparkConf()\
        .set("spark.local.dir","/dfs/scratch2/dankang/tmp")\
        .set("spark.network.timeout","10000001")\
        .set("spark.executor.heartbeatInterval","10000000")\
        .setMaster("local[*]")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.shuffle.partitions", "5000")
# ^^ This number can probably be lowered
# But the job ketp failing with 200 partitions, so just increased it a lot
# If needed to run again, would recommend trying from like 1000 partitions and adjust accordingly. 

WB_PATH = "/dfs/dataset/wb"
print(sc.getConf().getAll())

def timer(task, start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    time_text = "[{}]{:0>2}:{:0>2}:{:05.2f}\n".format(task, int(hours),int(minutes),seconds)
    print(time_text)

##########################################################

year = 2003
url_file_path = "/dfs/scratch2/dankang/wb_links/{}_filtered/urls_dedup.parquet".format(year)
link_file_path = "/dfs/scratch2/dankang/wb_links/{}/filtered_links_with_hash_no_dedup.parquet".format(year)
output_file_path = "/dfs/scratch2/dankang/wb_links/{}_filtered/filtered_links_with_hash_no_dedup.parquet".format(year)
output_hash_file_path = "/dfs/scratch2/dankang/wb_links/{}_filtered/hash_links.tsv".format(year)

#input_path = "/dfs/scratch2/dankang/WebGraph/data/2003_sample"
#target_path = "/dfs/scratch2/dankang/WebGraph/data/2003_sample_urls"

##########################################################

# Part 1: join the links with the urls we have
start = time.time()
print("Joining urls and links")
print("Reading urls from : " + url_file_path)
print("Reading links from : " + link_file_path)

urls = sqlContext.read.parquet(url_file_path)
links = sqlContext.read.parquet(link_file_path)

# We want to get links to pages that we ended up visiting
inner_join = urls.join(links, urls.Url == links.Dest, 'inner')\
                .select("Src","Dest", "Src_hash","Dest_hash")

inner_join.write.parquet(output_file_path)
end = time.time()
timer("Joining", start, end)

# Part 2: Use the link file created from above, save the deduped hash links
start = time.time()
sqlContext.read.parquet(output_file_path)\
        .select("Src_hash","Dest_hash")\
        .dropDuplicates()\
        .write.option("encoding", "UTF-8")\
        .csv(output_hash_file_path, sep="\t")

end = time.time()
timer("Dedup", start, end)
