import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive._

object MyMain extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Mini Project"))
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  sqlContext.sql("show databases").show
  sqlContext.sql("select * from t1").show
  println("Hello, world")
}
