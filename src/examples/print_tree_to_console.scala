package examples

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import h3.Tree
import h3.h3math._

object Main {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
               .setAppName("SparkMe Application")
               .setMaster("local")
               .set("spark.executor.memory","3g")
               .set("spark.driver.memory", "4g")
               
    val sc = new SparkContext(conf)

    /*
    if (args.length != 1) {
      println("Usage: /path/to/spark/bin/spark-shell Main referFile.json \n" + 
          "Or in shell: `:load Main referTraceFile.json`")
      sys.exit(1)
    }
		*/
    
    
    var treeClass = new Tree("data/tiny.json", sc)
    var tree = treeClass.tree
    //var tree = new Tree(path)
    println("tr :" + tree.numEdges)
    
  }
}
