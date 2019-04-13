import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("Sample").setMaster("local")

val sc = new SparkContext(conf)

val distFile = sc.textFile("example.arc.gz") 

