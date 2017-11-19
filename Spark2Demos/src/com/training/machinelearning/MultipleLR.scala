package com.training.machinelearning

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression


object MultipleLR {
  
  def main(args: Array[String]): Unit = {
    
  //Suppres the logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
  //Create the SparkSession
    val ss = SparkSession.builder()
                          .appName("Multiple Regression Demo")
                          .master("local[2]")
                          .config("spark.sql.warehouse.dir", "tmp/sparksql")
                          .getOrCreate()
  
   //Load the data
   val housingData = ss.read
                       .option("header", true)
                       .option("inferSchema", true)
                       .format("csv")
                       .load("data/housing.csv")
                       
   housingData.printSchema() 
   
  //Create a DF which contains labels & features (vectors)
   
   val df = housingData.select(col("Price").as("label"), 
       col("Avg Area Income"), 
       col("Avg Area House Age"), 
       col("Avg Area Number of Rooms"), 
       col("Avg Area Number of Bedrooms"), 
       col("Area Population"))
   
   //Use assembler to create vector of features
   val assembler = new VectorAssembler()
                  .setInputCols(Array("Avg Area Income",
                   "Avg Area House Age",
                   "Avg Area Number of Rooms",
                   "Avg Area Number of Bedrooms",
                   "Area Population"))
                   .setOutputCol("features")
                   
    //Transform the data such that you get (label, features) which can act as input to the algorithm 
     
    val finalData = assembler.transform(df).select(col("label"), col("features")) 
    finalData.take(2).foreach(println)
    
    //Create a LR Model
    val lr = new LinearRegression()
    val lrModel = lr.fit(finalData)
    lrModel.summary.residuals.show()
    
    lrModel.summary.predictions.show()
    println(lrModel.coefficients)
    println(lrModel.summary.coefficientStandardErrors)
    println(lrModel.summary.meanAbsoluteError)
    println(lrModel.summary.meanSquaredError)
  }
}