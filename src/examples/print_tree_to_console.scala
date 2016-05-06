package examples

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.io._
import scala.collection.mutable.Queue

import h3.Tree
import h3.h3math._

object Main {
  
  def print_tree(tree: Tree, node_id: Long, depth: Int = 0) {
      var children = tree.nodes(node_id).children
      var total_children_area: Double = 0
      println ("\t" * tree.nodes(node_id).depth +  
          "%d, parent: %d, radius: %f, area: %f"
          .format(node_id, 
                  tree.nodes(node_id).parent, 
                  tree.nodes(node_id).radius,
                  tree.nodes(node_id).area))
      for (child <- children) print_tree(tree, child.node_id, depth)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
               .setAppName("SparkMe Application")
               .setMaster("local")
//               .set("spark.executor.memory","3g")
//               .set("spark.driver.memory", "4g")
               
    val sc = new SparkContext(conf)

    /*
    if (args.length != 1) {
      println("Usage: /path/to/spark/bin/spark-shell Main referFile.json \n" + 
          "Or in shell: `:load Main referTraceFile.json`")
      sys.exit(1)
    }
		*/
    
    
//    var tree = new Tree("data/tiny.json", sc)
    var tree = new Tree("data/small.json", sc)
//    var tree = new Tree("data/medium.json", sc)
    //var tree = treeClass.tree
    //var tree = new Tree(path)
    //println("tr :" + tree.numEdges)
    print_tree(tree, 0)
    
    val out = new PrintWriter(new FileWriter("coordinates.csv"))
    for ((id, node) <- tree.nodes) {
        out.write(node.coord.x + ", " + 
                  node.coord.y + ", " + 
                  node.coord.z + "\r\n ")
    }
    out.close()
  }
}
