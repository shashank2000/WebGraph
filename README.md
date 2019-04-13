# WebGraph

## Setup
1. Log into server : @madmax4.stanford.edu
2. Clone this directory and `cd` into it
3. Run:`source setup.sh`. This will take a few minutes.
4. `cd` into lib
5. `git clone http://github.com/archivesunleashed/aut.git`
6. `cd` into `aut`
7. Call `mvn clean install -DskipTests`. This will take a few minutes.

## Verification
### Environment variables

Open file at `~/.bashrc.user`. It should include updates for:
`JAVA_HOME`, `SPARK_HOME`, `SPARK_LOCAL_IP`, `PATH`, `YARN_CONF_DIR`

### Spark
cd into `WebGraph/lib`, and then to spark directory

Call: `bin/spark --master local --packages "io.archivesunleashed:aut:0.17.0"`

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
Make sure you have finished step 7 (building mvn project)
From `aut` directory:

`pyspark --driver-class-path target/ --py-files target/aut.zip --jars target/aut-0.17.1-SNAPSHOT-fatjar.jar`

Once it is loaded, call the following.
Again, **Change path** accordingly as before.

```
from aut import *

path = "file:///afs/cs.stanford.edu/u/USERID/WebGraph/data/example.arc.gz"
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




