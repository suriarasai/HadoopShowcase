package transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author training
 */
object SparkTransformations {
  def main(args:Array[String]){ 
          val conf = new SparkConf 
                     conf.setMaster("local[2]").setAppName("Simple Trasnformations")
          val sc = new SparkContext(conf) 
          val baseRdd1 =     
          sc.parallelize(Array("hello","BA-BEAD","students","this","is","a",
          "this","is","a", "spark","scala", "demo"),1) 
          val baseRdd2 =      
          sc.parallelize(Array("eclispe","scala","spark","scala"),1) 
          val baseRdd3 =  sc.parallelize(Array(1,2,3,4),2)
          println("RDD1")
          baseRdd1.foreach(println)
          println("RDD2")
          baseRdd2.foreach(println)
          println("RDD3")
          baseRdd3.foreach(println)
          val sampledRdd = baseRdd1.sample(false,0.5) 
          println("SAMPLE")
          sampledRdd.foreach(println)
          val unionRdd = baseRdd1.union(baseRdd2).repartition(1) 
          println("UNION")
          unionRdd.foreach(println)
          val intersectionRdd = baseRdd1.intersection(baseRdd2) 
          println("INTERSECTION")
          intersectionRdd.foreach(println)
          val distinctRdd = baseRdd1.distinct.repartition(1) 
          println("DISTINCT")
          distinctRdd.foreach(println)
          val subtractRdd = baseRdd1.subtract(baseRdd2) 
          println("SUBSTRACT")
          subtractRdd.foreach(println)
          val cartesianRdd = sampledRdd.cartesian(baseRdd2) 
          println("CARTESIAN")
          cartesianRdd.foreach(println)
          val reducedValue = baseRdd3.reduce((a,b) => a+b) 
            
          val collectedRdd = distinctRdd.collect 
          println("UNION")
          collectedRdd.foreach(println) 
          
          val count = distinctRdd.count 
          val first = distinctRdd.first 
          println("Count is..."+count); 
          println("First Element is..."+first) 
          val takeValues = distinctRdd.take(3) 
          val takeSample = distinctRdd.takeSample(false, 2) 
          val takeOrdered = distinctRdd.takeOrdered(2) 
          takeValues.foreach(println) 
          println("Take Sample Values..") 
          takeSample.foreach(println) 
          val foldResult = distinctRdd.fold("<>")((a,b) => a+b) 
          println(foldResult) }
}