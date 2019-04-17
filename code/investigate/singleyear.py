import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from aut import *
import time

conf = SparkConf() \
    .set("spark.executor.memory", "4g") \
    .set("spark.executor.memoryOverhead", "4g") \
    .set("spark.executor.cores", 70)

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
WB_PATH = "/dfs/dataset/wb"

def timer(task, start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    time_text = "[{}]{:0>2}:{:0>2}:{:05.2f}\n".format(task, int(hours),int(minutes),seconds)
    print_and_log(time_text)

def print_and_log(text):
    print(text)
    write_log(text);

def write_log(text):
    with open(target_log_path, "a+") as f:
        f.write(text)

##########################################################

year = 2003
input_path = WB_PATH + "/{}/general/".format(year)
target_path = "/dfs/scratch2/dankang/wb_links/{}/general_links".format(year)

#input_path = "/afs/cs.stanford.edu/u/dankang/WebGraph/data/2003_sample"
#target_path = "/afs/cs.stanford.edu/u/dankang/WebGraph/data/2003_sample_links"
target_log_path = target_path + ".log"
target_file_path = target_path + ".parquet"


write_log("input_path: {}\n".format(input_path))
write_log("target_path: {}\n".format(target_path))
print_and_log("Configuration: {}\n".format(str(sc.getConf().getAll())))
write_log("=" * 30 + "\n\n")

all_start = time.time()

# Read data
print("Reading from : " + input_path)
print("=" * 10  + " Reading folder and ingesting to archive " + "=" * 10)
start = time.time()

archive = WebArchive(sc, sqlContext, input_path)

end = time.time()
timer("Ingest", start, end)
print("=" * 10  + " Finished ingesting to archive " + "=" * 10 + "\n\n")


# Extract links
print("=" * 10  + " Extracting links " + "=" * 10)
start = time.time()

links = archive.links()

end = time.time()
timer("Get Links", start, end)
print("=" * 10  + " Finished Extracting links" + "=" * 10 + "\n\n")

# Save at page level
print("=" * 10  + " Saving results " + "=" * 10)
start = time.time()

links.write.parquet(target_file_path)

end = time.time()
timer("Save Results", start, end)
print("=" * 10  + " Saved results " + "=" * 10 + "\n\n")
print("Saved to : " + target_file_path)


all_end = time.time()
timer("Everything: ", all_start, all_end)


count_text = "Saved total of : {} links\n".format(links.count())
print_and_log(count_text)
