package com.spark2scala.sparksqldemo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object StreamingSocketExample {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    val ss = SparkSession.builder()
                          .appName("Spark Streaming Demo")
                          .master("local[2]")
                          .config("spark.sql.warehouse.dir", "tmp/sparksql")
                          .getOrCreate()
    
    //Create SparkContext
    val sc =ss.sparkContext
    
    //Initialize Streaming Context
    val ssc = new StreamingContext(sc, Seconds(3))
    
    //Accept the receiver
    val receiver =ssc.socketTextStream("172.16.59.130", 3000)
    
    //print the Data Stream
    receiver.print()
    
    //Start the streaming  process
    ssc.start()
    ssc.awaitTermination()
  }
}