package com.spark2scala.sparksqldemo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterExample {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    val ss = SparkSession.builder()
                          .appName("Twitter Demo")
                          .master("local[2]")
                          .config("spark.sql.warehouse.dir","tmp/sparksql")
                          .getOrCreate()
    
    //Create SparkContext for Initializing StreamingContext
    val sc = ss.sparkContext
    
    //Initialize Streaming Context
    val ssc = new StreamingContext(sc, Seconds(3))
    
    //Configure Twitter Credentials
    System.setProperty("twitter4j.oauth.consumerKey", "2khjAMRVX3lu53fW0gZI2kcVv")
    System.setProperty("twitter4j.oauth.consumerSecret", "La3WMgAb7FaaG7Adeav3LR4zDh1hfSPZOogZxY4PiQVo7ubDCs")
    System.setProperty("twitter4j.oauth.accessToken", "69000990-Q71s2jyPnCcIUiuCdhas6CIUY1Xy1BVrmi1ZEzpZ2")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "LHcf9QZNbsQjmGJCH9prDRl2eLDfZ3SDrGDJpkeyLuEOY")
    
    
    //Connect to Twitter
    
   // val tweet = TwitterUtils.createStream(ssc, None)
    
    //Get the tweet text
    
    //val tweetData = tweet.
  }
}