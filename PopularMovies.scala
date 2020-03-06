package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.util.control.Breaks._
/** Find the movies with the most ratings. */
object PopularMovies {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
      def parseLine(line: String) = {
      val fields = line.split("\t")
      // Extract the age and numFriends fields, and convert to integers
      val userID = fields(0).toInt
      val movieID = fields(1).toInt
      val ratings = fields(2).toInt
      // Changed made here 
      (movieID)
  }
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMovies")   
    
    // Read in each rating line
    val lines = sc.textFile("../ml-100k/u.data")
    
    // Map to (movieID, 1) tuples
    val getFields = lines.map(parseLine)
    val movieCount = getFields.map(x=>(x,1))
    val movieRated = movieCount.reduceByKey((x,y)=>x+y)
    val movieRatedSwitched = movieRated.map(x=>(x._2,x._1))
    val movieListSorted = movieRatedSwitched.sortByKey(false).collect
    var j = 1
      for (i <- movieListSorted){
         val mID = i._2
         val numberRated = i._1
         if (numberRated <= 200) break
         println(s"Rank : ${j}  movie ${mID} ; Rated ${numberRated} times")
         j = j + 1
      }
  }
  
}

