package data

import java.io.StringReader
import java.io.StringWriter

import scala.collection.JavaConverters._
import scala.collection.JavaConverters._

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import com.fasterxml.jackson.databind.ObjectMapper

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter

object Loading_SavingData {
   case class Person(name:String, age:Int)

  case class Stocks(name:String, totalPrice:Long)

  def main(args:Array[String]): Unit = {

    val conf = new SparkConf
    conf.setMaster("local[*]").setAppName("Loading_SavingData")
    val sc = new SparkContext(conf)

   val mapper = new ObjectMapper()
    // Loading text file
    val input =
      sc.textFile("hdfs://localhost:8020/user/training/input.txt")
    val wholeInput = sc.wholeTextFiles("/home/training/training_materials/data/")

    val result = wholeInput.mapValues{value => val nums = value.split(" ").map(x => x.toDouble)
      nums.sum/nums.size.toDouble}

    result.saveAsTextFile("/home/training/training_materials/data/outputFileWholeInput")

// Loading JSON File
    val jsonInput = sc.textFile("/home/training/training_materials/dev1/examples/example-data/people.json")
      val result1 = jsonInput.flatMap(record => {
      try{Some(mapper.readValue(record, classOf[Person]))
      }
      catch{
      case e:Exception => None
      }} )
      result1.filter(person => person.age>15).map(mapper.writeValueAsString(_)).
      saveAsTextFile("/home/training/data/outputFile")



    // Loading CSV

    val input1 = sc.textFile("/home/training/git/HadoopShowcase/SimpleSparkExamples/data/uber.csv")
    val result2 = input1.flatMap{line => val reader = new CSVReader(new
        StringReader(line))
      reader.readAll().asScala.toList.map(x => Stocks(x(0), x(3).toLong))
    }

    result2.foreach(println)
    result2.map(stock => Array((stock.name, stock.totalPrice))).mapPartitions {stock =>
      val stringWriter = new StringWriter
      val csvWriter = new CSVWriter(stringWriter)

      csvWriter.writeAll(stock.toList.map(arr => arr.map(x => x._1+x._2.toString)).asJava)
      Iterator(stringWriter.toString)
    }.saveAsTextFile("/home/training/data/outputFile/CSVOutputFile")

    //Loading Sequence File
    
    val data = sc.sequenceFile("/sequenceFile/path", classOf[Text],
      classOf[IntWritable]).map{case(x,y) => (x.toString, y.get())}
    val input3 = sc.parallelize(List(("Panda",3),("Kay",6),
      ("Snail",2)))
    input3.saveAsSequenceFile("hdfs://localhost:8020/user/training/sequenceOutputFile")
  }
}