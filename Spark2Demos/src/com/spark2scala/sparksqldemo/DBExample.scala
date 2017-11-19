package com.spark2scala.sparksqldemo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.util.Properties

object DBExample {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    val ss = SparkSession.builder()
                         .appName("GetDataFromDataBase")
                         .master("local")
                         .config("spark.sql.warehouse.dir", "tmp/sparksql")
                         .getOrCreate()
                         
    //load data from database
                         
    val prop = new Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    
    val dbData = ss.read
                    .jdbc("jdbc:mysql://172.16.59.130:3306/sparkclass", "emp", prop)
     
      dbData.printSchema()
      dbData.show()
  }
}