package code.aws.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object StringFlatMapRDD extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "StringManupRdd")
  val orderRDD=sc.textFile("F://dataFrom//data-master//retail_db//a.txt")
  //orderRDD.take(10).foreach(println)
  val orderRDDMap=  orderRDD.map(ele => ele.split(" "))
  val orderRDDFMap=  orderRDD.flatMap(ele => ele.split(" "))
  val countWord=orderRDDFMap.map(word=>(word,"")).countByKey()
  countWord.take(100).foreach(println)



}
