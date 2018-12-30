package code.aws.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object ReduceAndAggregateByKeyRDD extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "ReduceAndAggregateByKeyRDD")
  val orderRDD=sc.textFile("F://dataFrom//data-master//retail_db//orders")
  val orderItemsRDD=sc.textFile("F://dataFrom//data-master//retail_db//order_items")
 // val orderMap = orderRDD.map(x=> (x.split(",")(3),"")).countByKey()
  //orderMap.take(10).foreach(println)
  orderItemsRDD.take(10).foreach(println)

  val orderItemsRevenue = orderItemsRDD.map(oi => oi.split(",")(4).toFloat)
  val revnue=orderItemsRevenue.reduce((total, revenue) => total + revenue)
  println(revnue)
  val orderItemMaxRevenue =orderItemsRevenue.reduce((x,y)=>{
    if (x < y) y else x
  })
  println(orderItemMaxRevenue)


}
