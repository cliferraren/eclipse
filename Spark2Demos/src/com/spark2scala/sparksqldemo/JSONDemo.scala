package com.spark2scala.sparksqldemo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object JSONDemo {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    val ss = SparkSession.builder()
                         .appName("JSON Demo")
                         .master("local")
                         .config("spark.sql.warehouse.dir", "tmp/sparksql")
                         .getOrCreate()
   
    val dataJSON = ss.read
                      .option("inferSchema", "true")
                     .json("data/customerData.json")
    
    dataJSON.show()
    dataJSON.printSchema()
    
    //Select query
    
    dataJSON.select(col("name"),col("salary")).show()
    
    //Filter Query ==40
    
    dataJSON.filter(col("age").equalTo(40)).show()
    
    //Filter age between 30 and 40
    
    dataJSON.filter(col("age").between(30, 40)).show()
    
    //filter age greater than or equal to 26
    
    dataJSON.filter(col("age").>=(40)).show()
    
    //Group and Aggregation (group by deptid and find the average salary and max age)
    
    dataJSON.groupBy("deptid").agg(avg(dataJSON.col("salary")), max(dataJSON.col("age"))).show()
        
    
  }
}