import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from aut import *

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
###======================================================================###

# Create a Vertex DataFrame with unique ID column "id"
v = sqlContext.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
    ], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = sqlContext.createDataFrame([
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
    ], ["src", "dst", "relationship"])

# Create a GraphFrame
from graphframes import *
g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
print(g.edges.filter("relationship = 'follow'").count())

# Run PageRank algorithm, and show results.

#bfs = g.bfs.fromExpr("id = 'a'").toExpr("id <> 'a'")
bfs = g.bfs("id = 'a'", "id = 'c'")
bfs.show()

