import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from aut import *

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
WB_PATH = "/dfs/dataset/wb"

##########################################################

#file_path = WB_PATH + "/2003/general/2003-10_ae_net_emirates_www_1_64515.warc.gz"  # 1412 bytes
#file_path = WB_PATH + "/2003/general/2003-10_ae_org_uae_www_1_64494.warc.gz" # 55,072 bytes, 31 pages
#file_path = WB_PATH + "/2003/general/2003-10_ai_gov_www_1_64504.warc.gz" # 2,897,638 bytes, 748 pages
#file_path = WB_PATH + "/2003/general/2003-10_ae_gov_uae_www_1_64467.warc.gz" # 31,463,983 bytes, 8156 pages
file_path = "/dfs/scratch2/dankang/WebGraph/data/2003_sample"
#file_path = "/afs/cs.stanford.edu/u/dankang/WebGraph/data/2003_sample/"
archive = WebArchive(sc, sqlContext, file_path)

links = archive.links()
links.printSchema()

# This is printing at domain level
'''
links.select(extract_domain("Src").alias("SrcDomain"), extract_domain("Dest").alias("DestDomain")) \
            .groupBy("SrcDomain", "DestDomain").count().orderBy("count", ascending=False).show()
'''


# This is printing at page level
links.show()
print("Saving results")
target_path = "/dfs/scratch2/dankang/WebGraph/data/2003_sample_linkes.parquet"
#target_path = "/afs/cs.stanford.edu/u/dankang/WebGraph/data/2003_sample_page_level.parquet"
links.write.parquet(target_path)

print("Reading from saved results")
links2 = sqlContext.read.parquet(target_path)
links2.show()
#links.filter(custom_is_link(links["Src"]) & custom_is_link(links["Dest"])).show()
#links.select(custom_extract_url("Src").alias("SrcUrl"), custom_extract_url("Dest").alias("DestUrl")).show()
#links.groupBy("Src","Dest").count().orderBy("count").show()

