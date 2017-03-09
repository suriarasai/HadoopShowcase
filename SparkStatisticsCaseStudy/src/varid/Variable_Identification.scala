package varid

/**
 * @author training
 */
import org.apache.spark._
import org.apache.spark.sql._

object Variable_Identification {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Variable_Identification")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val students_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:8020/user/training/students.csv")
    students_data.show(5)
  }
}