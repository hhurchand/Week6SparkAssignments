package com.cellariot.spark
// H Hurchand
// For Temperature Exercise - all 3 changes are on this file


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max
import scala.math.abs


/** Find the maximum temperature by weather station for a year */
object MaxTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val date = fields(1)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature, date)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    
    val lines = sc.textFile("../1800.csv")
    val parsedLines = lines.map(parseLine)
    // RDD for max Temp
    val maxTemps = parsedLines.filter(x => x._2 != "PRCP" )
    val tempByIndex = parsedLines.map(x => ((x._1,x._4),x._3))
    val maxDiff  = tempByIndex.reduceByKey((x,y) => abs(x-y))
    val pickMaxDiff = maxDiff.reduceByKey((x,y) => max(x,y))
    val pickMaxofAll = pickMaxDiff.reduce((x,y)=> if(x._2>y._2) x else y)
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    val maxTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y))
    val results = maxTempsByStation.collect()
    
        for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"Exercise 1: $station max temperature: $formattedTemp") 
    }
    
    println(s"Exercise 2 : Maximum temperature difference ${pickMaxofAll._2} at station ${pickMaxofAll._1._1}")
    
    println("Exercise 3 : Station and Time of biggest weather change "+pickMaxofAll)
    
    
    

    
      
  }
}