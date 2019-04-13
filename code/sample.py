import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from aut import *

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

home = os.environ['WEBGRAPH_HOME']

path = home + "/data/example.arc.gz"
archive = WebArchive(sc, sqlContext, path)
pages = archive.pages()
pages.printSchema()

pages.select(extract_domain("Url").alias("Domain")) \
            .groupBy("Domain").count().orderBy("count", ascending=False).show()


