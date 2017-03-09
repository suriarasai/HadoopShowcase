package basics
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author training
 */
object SparkContextEg {
  def main(args: Array[String]) {
    val txtPath = "hdfs://localhost:8020/user/training/input.txt"
    val conf = new SparkConf().setAppName("Counting Lines").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.textFile(txtPath, 2)
    val totalLines = data.count()
    println("Total number of Lines: %s".format(totalLines))
  }
}