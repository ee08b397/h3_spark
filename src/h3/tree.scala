package h3

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph.graphToGraphOps

import h3.h3math

class Tree(val path: String, val sc: SparkContext){

  var tree = load(path)

  def load(path: String) : Graph[_, _] = {

    val strRDD = sc.textFile(path)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val jsonDf = sqlContext.jsonRDD(strRDD)
    jsonDf.printSchema()
    jsonDf.show()

    import sqlContext.implicits._

    // all child nodes
    var childArray = jsonDf.select($"child").rdd.map(r => 
      (r(0).asInstanceOf[Long])).collect()

    // all parent nodes
    var parentArray = jsonDf.select($"parent").rdd.map(r => 
      (r(0).asInstanceOf[Long])).collect()

    // set ancestor node, add to parentArray
    val ancestor = math.max(parentArray.max, childArray.max) + 1

    var childTupleArray = childArray.map(t => t.toLong -> "child")
    var parentTupleArray = parentArray.map(t => t.toLong -> "parent")

    //var verticesArray:Array[(Long, String)] = _
    var verticesArray = parentTupleArray.union(childTupleArray).distinct
    var root: Long = 0
    val leaf_nodes = childArray.diff(parentArray).toSet
    val root_node = parentArray.diff(childArray).toSet

    if (root_node.size > 0) {
      var completeParentTupleArray = parentTupleArray :+ (ancestor, "ancestor")

      // add all child nodes, parent nodes and ancestor together, deduplicate
      verticesArray = completeParentTupleArray.union(childTupleArray).distinct
      root = ancestor
    } else {
      verticesArray = parentTupleArray.union(childTupleArray).distinct
      root = root_node.toList(0)
    }

    // var verticesArray = completeParentTupleArray.union(childTupleArray).distinct
    // var verticesArray = parentTupleArray.union(childTupleArray).distinct


    // convert to RDD
    val verticesRdd: RDD[(VertexId, String)] = sc.parallelize(verticesArray)



    // edges RDD
    var allEdges = jsonDf.select($"child", $"parent").distinct.rdd.map(r => 
        (r(0).asInstanceOf[Long], r(1).asInstanceOf[Long])
    ).collect()

    var acyclic = allEdges.map(x => (x._1, x._2)).toMap.toList

    var edgeArray = acyclic.map(r =>
      Edge(
        r._1.asInstanceOf[Long], 
        r._2.asInstanceOf[Long], 
        "referer"
      )
    )

    //scala> acyclic.size
    //res99: Int = 3155
    //scala> edgeArray.length
    //res100: Int = 3184

    // orphants
    val orphantArray = parentArray.filterNot(childArray.toSet).toSet.toList
    var orphantTupleArray = orphantArray.map(t => Edge(t.toLong, ancestor.toLong, "virtual"))

    // found cycles
    // jsonDf.groupBy("child").agg(count("parent") > 1).collect.foreach(println)

    var edgesArray = edgeArray.union(orphantTupleArray).distinct

    val edgeRdd: RDD[Edge[String]] = sc.parallelize(edgeArray)

    val graph = Graph(verticesRdd, edgeRdd)

    return graph
  }
}


