
import org.apache.spark._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.log4j.PropertyConfigurator


object WordCount {
  def main(args: Array[String]) {
      
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[1]"))
    val logFile = "file:/home/zsx/loadOOD.scala"
    val logData = sc.textFile(logFile, 2).cache()
    
    val numAs = logData.filter(line => line.contains("a")).count()
    
    println("lines with 'a' " + numAs)
  }
}