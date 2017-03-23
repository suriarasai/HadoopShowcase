package sample.stats

/**
 * @author training
 */
import org.apache.spark._
import org.apache.spark.sql._ 
import org.apache.spark.sql.functions._

object Summary_Statistics {
  
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Summary_Statistics")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val loan_Data =          
      sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load("hdfs://localhost:8020/user/training/Loan_Prediction_Data.csv")
      // Gets summary of all numeric fields           
     val summary = loan_Data.describe()
     summary.show()           
     // Get Summary on subset of columns           
     val summary_subsetColumns =            
       loan_Data.describe("ApplicantIncome", "Loan_Amount_Term")
       summary_subsetColumns.show()       
       // Get subset of statistics
     val subset_summary = 
       loan_Data.select(mean("ApplicantIncome"), min("ApplicantIncome"), max("ApplicantIncome"))       
     subset_summary.show() 
           
 }
 
}