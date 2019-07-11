# WebGraph

## Directory Structure
- code 
    - analysis: jupyter notebook files for visualization
    - investigate: main files to generate webgraphs and different results
- data: folder that could be used to store small data files for testing purposes
        could copy a few files from a single year and use it as a small sample
        One can just rename given `data_sample` directory to `data`
- lib: library folder to install local framework. can include webgraph, apache-maven, aut, and spark    
- etc: some configuration files that are handy
- setup.sh: file to run for initial setup - make sure to read through / definitely not 100% tested


## Setup
1. Log into server : @madmax4.stanford.edu
2. Clone this directory and `cd` into it
3. Run:`source setup.sh`. This will take a few minutes.

## Webgraph
1. Download the jar and dependencies in `lib/webgraph` directory
2. When running java files, specify classpath
e.g. `java -cp "/dfs/scratch2/dankang/WebGraph/lib/webgraph/*" .... `


## Verification
### Environment variables

Open file at `~/.bashrc.user`. It should include updates for:
`WEBGRAPH_HOME`, `JAVA_HOME`, `SPARK_HOME`, `SPARK_LOCAL_IP`, `PATH`, `YARN_CONF_DIR`,`M2_HOME`, `AUT_PATH`

### Spark
cd into `WebGraph/lib`, and then to spark directory

Call: `bin/spark-shell --master local --packages "io.archivesunleashed:aut:0.17.0"`

When the spark shell comes up:

1. Type `:paste` to get into paste mode
2. Copy-paste the following code.
**Change path** so that it points to `WebGraph/data/example.arc.gz`. (likely only change USERID portion)
Note that we start with `"file://"` to specify we are using a local directory.
```
import io.archivesunleashed._
import io.archivesunleashed.matchbox._

val path = "file:///afs/cs.stanford.edu/u/USERID/WebGraph/data/example.arc.gz"
val r = RecordLoader.loadArchives(path, sc)
.keepValidPages()
.map(r => ExtractDomain(r.getUrl))
.countItems()
.take(10)
```
3. Press `ctrl + d` to execute. The result should look something like:
`r: Array[(String, Int)] = Array((www.archive.org,132), (deadlists.com,2), (www.hideout.com.br,1))`

### Pyspark
From `lib/aut` directory:

`pyspark --driver-class-path target/ --py-files target/aut.zip --jars target/aut-0.17.1-SNAPSHOT-fatjar.jar`

Once it is loaded, call the following.
Again, **Change path** accordingly as before.

```
from aut import *

path = "file:////afs/cs.stanford.edu/u/USERID/WebGraph/data/example.arc.gz"
archive = WebArchive(sc, sqlContext, path)
pages = archive.pages()
pages.printSchema()

pages.select(extract_domain("Url").alias("Domain")) \
    .groupBy("Domain").count().orderBy("count", ascending=False).show()
```
Should return results that are similar to:
```
+--------------+-----+
|        Domain|count|
+--------------+-----+
|   archive.org|  132|
| deadlists.com|    2|
|hideout.com.br|    1|
+--------------+-----+
```

### Pyspark-submit
Try running :
`spark-submit ~/WebGraph/code/sample.py`

It should print out similar output as above (Pyspark)


