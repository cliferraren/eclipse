package com.training.machinelearning

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._

object SpamfilteringDemo {
  def main(args: Array[String]):Unit ={
    
    //Suppress the logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    //Create the SparkSession
    val ss = SparkSession.builder()
                          .appName("Email spam Detection Demo Application")
                          .master("local[2]")
                          .config("spark.sql.warehouse.dir", "tmp/sparksql")
                          .getOrCreate()
           
                       
     //Create a Sample Dataset for Email Spam
     //0 ---HAM
     //1 ---SPAM                     
     val trainingDataSet = ss.createDataFrame(
         Seq(
            ("cliferraren@gmail.com", "Hope you are well",0.0),
            ("rajat@gmail.com", "nice to hear from you",0.0),
            ("ads@gmail.com", "Low Interest Rates",1.0),
            ("mark@example.com", "Cheap Loans",1.0),
            ("xyz@gmail.com", "See you tomorrow",0.0),
            ("cliferraren@mail.com", "Welcome to Spark",0.0),
            ("rajat@tcs.com", "Learn Machine learning",0.0),
            ("ads@rediff.com", "Low Interest Rates",1.0),
            ("mark@aaha.com", "gold Investment",1.0),
            ("xyz@yahoo.com", "Adigos",0.0)
         )
     ).toDF("emailid","subject","label")  
     
     //Tokenizer is a class which is used to accept the input(feature) as a Non-NUMERIC data
     //Create a Machine Learning Pipeline which holds 
     //a. Tokenize
     //b. Hashing TermFrequency
     //c. Applying lr model
     
     val tokenizer = new Tokenizer().setInputCol("subject").setOutputCol("words")
     val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words")
                     .setOutputCol("features")
     
     //Create a LogisticRegression Model
     val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
     
     //Initialize the pipeline
     val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF,lr))
     
     //Fit the data to the model
     val model = pipeline.fit(trainingDataSet)
     
     //Let's test the model
     
     val testDataSet = ss.createDataFrame(
         Seq(
            ("cliferraren@gmail.com", "Hope you are well"),
            ("rajat@gmail.com", "nice to hear from you"),
            ("ads@gmail.com", "Low Interest Rates"),
            ("mark@example.com", "Cheap Loans"),
            ("xyz@gmail.com", "See you tomorrow"),
            ("cliferraren@mail.com", "Welcome to Spark"),
            ("rajat@tcs.com", "Learn Machine learning"),
            ("ads@rediff.com", "Low Interest Rates"),
            ("mark@aaha.com", "gold Investment"),
            ("xyz@yahoo.com", "Adigos"),
            ("ramky@spicejet.com", "Low Interest on Airticket"),
            ("a@a.com", "Cheaper Solutions")
         )
     ).toDF("emailid","subject")
     
  val pred = model.transform(testDataSet).select(col("emailid"), col("subject"), col("prediction"))   
  pred.show()
  }
}