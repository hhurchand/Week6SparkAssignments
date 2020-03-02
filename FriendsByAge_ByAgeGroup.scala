/** Compute the average number of friends by age groups (in 10s)**/
// H Hurchand

package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._



object FriendsByAge_ByAgeGroup {
  
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      val firstName = fields(1)
      // Create a tuple that is our result.
      (age,numFriends)
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../fakefriends.csv")
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    // Lots going on here...
    // RDD1 -- tuple (AgeGroup in 10s, Number of Friends)
    
    val totalsByAge = rdd.map(x => (x._1-x._1%10,x._2))
    
    // RDD2 -- tuple (Number of Friends,1) for each key - AgeGroup is key
    
    val Friends_Count = totalsByAge.mapValues(x=>(x,1))
    // RDD3 add all friends and their occurrences
    val sumFriends = Friends_Count.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    // Compute Average
    val averagesByAge = sumFriends.mapValues(x => x._1 / x._2)
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByAge.collect()
    
    // Sort and print the final results.
    for (i <- results.sorted){
      println("Age Group  :" + i._1,"Average :"+i._2)
    }
    
  }
    
}
  