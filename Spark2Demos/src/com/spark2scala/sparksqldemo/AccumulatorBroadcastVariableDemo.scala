package com.spark2scala.sparksqldemo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object AccumulatorBroadcastVariableDemo {
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    val ss = SparkSession.builder()
                          .appName("Accumulator Broadcast Variable Demo")
                          .master("local")
                          .config("spark.sql.warehouse.dir","tmp/sparksql")
                          .getOrCreate()
                          
    //how to create a create a SparkContext Object
    val sc = ss.sparkContext                      
                          
    //Count of Sedan and Hatchback
    //Function called splitManual ---> Update accumulator variables of Sedan and Hatchback
    
    //Load the data
    val autodataRDD = sc.textFile("data/auto-data.csv", 4)
    
    //Initialize Accumulator
    //collectionAccumulator, doubleAccumulator
    var myGlobalSedanVariable = 0
    var myGlobalHBVariable =0
    
    val sedancount = sc.longAccumulator
    val hbCount = sc.longAccumulator
    
    //Initialize Broadcast Variable - Read-only
    val bsedan =sc.broadcast("sedan")
    val bhback = sc.broadcast("hatchback")
    
    //function in scala -- Must always return a value
    
    def splitlines(lines: String):Array[String] = {
      if(lines.contains(bsedan.value))
          {
            sedancount.add(1)
            myGlobalSedanVariable += 1
            
          }
      if(lines.contains(bhback.value))
          {
            hbCount.add(1)
            myGlobalHBVariable +=1
          }
      lines.split(",")
      }
    //val autodatasplit = autodataRDD.map(splitlines)
    val autodataSplit = autodataRDD.map(record=>splitlines(record))
    
    autodataSplit.count()
    
    println("Sedan count: " +sedancount.value)
    println("HatchBack count: "+hbCount.value)
    
    println("Sedan count from global variable: " +myGlobalSedanVariable)
    println("HatchBack count from global variable: " +myGlobalHBVariable)
    }
}