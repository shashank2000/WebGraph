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
input_file_name = "filtered_links_with_hash_no_dedup.parquet"
output_hash_file_name = "domain_links_with_hash_and_counts.parquet"
output_file_name = "domain_hash_links.tsv"
file_dir = "/dfs/scratch2/dankang/wb_links/2007/"

# FOR TESTING
#file_dir = "/dfs/scratch2/dankang/WebGraph/data/"
#input_file_name = "2003_filtered_links_with_hash_no_dedup.parquet"
#output_hash_file_name = "2003_domain_links_with_hash_and_counts.parquet"
#output_file_name = "2003_domain_hash_links_num.tsv"

input_file_path = file_dir + input_file_name
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

def extract_domain_func(url):
    # Note that at this stage, we already have everything up to (including) 'www' removed
    if '/' in url:
        return url.split('/')[0]
    else:
        return url

extract_domain = udf(extract_domain_func, StringType())

###===========================================#######

all_start = time.time()
announce_task("Read in parquet file")

filenum = len(os.listdir(input_file_path))
print("There are {} files".format(filenum))

links = sqlContext.read.parquet(input_file_path).select("Src","Dest")
domain_start = time.time()
timer("Loading base parquet file", all_start, domain_start)

announce_task("Extract domains and hash it")

#links.show()


# Extract domain, get hash values
links = links.withColumn("Src_domain", extract_domain(col("Src")))\
            .withColumn("Dest_domain", extract_domain(col("Dest")))\
            .select("Src_domain", "Dest_domain")\
            .groupBy("Src_domain", "Dest_domain").count()\
            .filter("Src_domain != ''")\
            .filter("Dest_domain != ''")


#links.show()

links = links.withColumn("Src_domain_hash", xxhash_udf(col("Src_domain")))\
            .withColumn("Dest_domain_hash", xxhash_udf(col("Dest_domain")))

#links.show()

# Save the whole links with hash values
links.write.parquet(output_hash_file_path)

link_start = time.time()
timer("Extracting domains and saving links with hash values", domain_start, link_start)

announce_task("Save hashed links only")

hashed_domain_links = sqlContext.read.parquet(output_hash_file_path)\
                        .select("Src_domain_hash","Dest_domain_hash")\
                        .write.option("encoding", "UTF-8")\
                        .csv(output_file_path, sep="\t")

all_end = time.time()
timer("Saving hashed links only", link_start, all_end)
timer("Total", all_start, all_end)
