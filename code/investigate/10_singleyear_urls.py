import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col, lit, regexp_replace, udf
from functools import reduce
from aut import *
import time
import argparse

conf = SparkConf()

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
WB_PATH = "/dfs/dataset/wb"
print(sc.getConf().getAll())

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


##########################################################

year = 2003
input_path = WB_PATH + "/{}/general/".format(year)
target_path = "/dfs/scratch2/dankang/wb_links/{}_filtered/urls".format(year)

#input_path = "/dfs/scratch2/dankang/WebGraph/data/2003_sample"
#target_path = "/dfs/scratch2/dankang/WebGraph/data/2003_sample_urls"
target_file_path = target_path + ".parquet"
target_log_path = target_path + ".log"

write_log("input_path: {}\n".format(input_path))
write_log("target_path: {}\n".format(target_path))
print_and_log("Configuration: {}\n".format(str(sc.getConf().getAll())))

# Read data
print("Reading from : " + input_path)
start = time.time()

archives = WebArchive(sc, sqlContext, input_path)
urls = archives.pages()\
    .select('Url')

VALID_STARTS = ["http", "www"]
def starts_with(col_name):
    return reduce(lambda x, y: x | y, [(col(col_name).startswith(s)) for s in VALID_STARTS], lit(False))
urls = urls.where(starts_with("Url"))\
    .withColumn("Url", regexp_replace(col("Url"), "\\s+", ""))\
    .withColumn("Url", normalize_url(col("Url")))

#.dropDuplicates()

urls.write.parquet(target_file_path)

end = time.time()
timer("Total", start, end)
