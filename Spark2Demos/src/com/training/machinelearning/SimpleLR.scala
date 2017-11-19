package com.training.machinelearning

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors


object SimpleLR {
  def main(args: Array[String]): Unit = {
    
    
    //Suppress the logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    
    //Create the SparkSession
    val ss = SparkSession.builder()
                          .appName("Machine Learning Demo")
                          .master("local[2]")
                          .getOrCreate()
    
     //Load the Data
     val housingSimpleData = ss.read
                               .option("header", true)
                               .option("inferSchema", true)
                               .format("csv")
                               .load("data/SimpleLRDataset")
                               
                               
      housingSimpleData.printSchema()
     
      //Prepare my data to apply Linear Regression Algorithm
      //Create Labels and Features. In our case label= price; Features = []--Vector
      //You will pick only the relevant columns which satisfy the feature.
      
      //Load the data
      val df = housingSimpleData.select(col("price").as("label"), col("size"))
      df.printSchema()
      
      //Assemble - Responsible to collect the feature variables
      
      //Spark ML provides VectorAssembler to achieve this task(VectorAssembler)
      
      val assembler = new VectorAssembler().setInputCols(Array("size"))
                                            .setOutputCol("features")
                                            
     ///Transform the data to the assembler
                                            
      val finalData = assembler.transform(df).select(col("label"), col("features"))     
      finalData.printSchema()
      finalData.show()
      
      //Build a model using Linear Regression Algorithm
      
      val lrModel = new LinearRegression()
      
      //train my model to provide predictions
      //App will predict the price based on the aread of the house (sqft)
    
      val lrModelExperience= lrModel.fit(finalData)
    
      val learned = lrModelExperience.summary
      
      //Residual defines the performance between the actual and the predicted label
      learned.residuals.show()
     
      //To show the predicted values of the model
      learned.predictions.show()
    
      println("Improvising!!!!!!!!!")
      
      //Create a Training and Testing Set for Model Building
      
      val Array(training,testing) = finalData.randomSplit(Array(0.75,0.25),seed=4000)
      
      println("Training Data......")
      training.show()
      
      println("Testing Data.......")
      testing.show()
      
      //Creating new Model
      println("Creating a New Model......")
      
      val model2 = lrModel.fit(training)
      model2.summary.predictions.show()
      
      println("Test the new Model")
      val test2 = model2.transform(testing)
      test2.show()
  
     //Lets give 5000 as the feature and get the predicted price from the model
      
      val newData = ss.createDataFrame(
                  Seq(Vectors.dense(5000)).map(Tuple1.apply)
      
      ).toDF("features")
      
      newData.show()
      
      lrModelExperience.transform(newData).show()
      model2.transform(newData).show()
  }
}