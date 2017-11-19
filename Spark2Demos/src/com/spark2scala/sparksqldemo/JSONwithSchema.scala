package com.spark2scala.sparksqldemo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes

import org.apache.spark.sql.functions._

object JSONwithSchema {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    
    val ss = SparkSession.builder()
                          .appName("JSON with Schema")
                          .master("local")
                          .config("spark.sql.warehouse.dir","tmp/sparksql")
                          .getOrCreate()
    
    //create the schema for customerData
    val schemaCustData = StructType(
                  Array(
                      StructField("age", DataTypes.IntegerType, true),
                      StructField("deptid", DataTypes.IntegerType, true),
                      StructField("gender", DataTypes.StringType, true),
                      StructField("name", DataTypes.StringType, true),
                      StructField("salary", DataTypes.IntegerType, true)
                    )
     )
    
    //create the schema for customerCity
     val schemaCustCity = StructType(
                   Array(
                       StructField("deptid", DataTypes.IntegerType, true),
                       StructField("store_location", DataTypes.StringType, true)
                       )
     )            
                       
    //Load JSON data
    val jsonCustData = ss.read
                    //.schema(schemaCustData)
                    .json("data/customerData.json")
    
    val jsonCustCity = ss.read
                   // .schema(schemaCustCity)
                    .json("data/customerCity.json")
                    
    jsonCustData.printSchema()
    jsonCustCity.printSchema()
    
    
    val joinData = jsonCustData.join(jsonCustCity, jsonCustData.col("deptid").equalTo(jsonCustCity.col("deptid")))
    joinData.show()
    
  }
}