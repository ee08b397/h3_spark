package h3

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph.graphToGraphOps

import org.slf4j._
import scala.collection.mutable.Queue

import h3.h3math._
import h3.Node
import Config.NULL_PARENT

class Tree(val path: String, val sc: SparkContext){
  
    //var tree = load(path)
    var nodes: Map[Long, Node] = Map()
    var height: Int = 0
    var root: Node = null
    val logging = LoggerFactory.getLogger(classOf[Tree])
    var graph: Graph[_, _] = load(path)
    
    def load(path: String): Graph[_, _] = {
  
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
        var orphantTupleArray = orphantArray.map(t => Edge(t.toLong, 
                ancestor.toLong, "virtual"))
    
        // found cycles
        // jsonDf.groupBy("child").agg(count("parent") > 1).collect.foreach(println)
    
        var edgesArray = edgeArray.union(orphantTupleArray).distinct
        val edgeRdd: RDD[Edge[String]] = sc.parallelize(edgeArray)
        val graph = Graph(verticesRdd, edgeRdd)
    
        return graph
    }

    init(path)
    
    def init(path: String) {
        val strRDD = sc.textFile(path)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val jsonDf = sqlContext.jsonRDD(strRDD)
    
        import sqlContext.implicits._
        
        var allEdges = jsonDf.select($"child", $"parent").distinct.rdd.map(r => 
            (r(0).asInstanceOf[Long], r(1).asInstanceOf[Long])
        ).collect()
        
        allEdges.foreach{ e =>
            val (child_id: Long, parent_id: Long) = e
            insert_edges(parent_id, child_id)
//            println (parent_id + " " + child_id)
        }
        
        for ((id, node) <- nodes) {
            if (node.parent == NULL_PARENT) {
                root = node
            }
        }
        
        label_node_generations()
        print_tree()
        set_subtree_radius()
        print_tree()
        set_subtree_size()
        print_tree()
        sort_children_by_radius()
        print_tree()
        set_placement()
        print_tree()
    }
    
    def insert_edges(parent_id: Long, child_id: Long): (Node, Node) = {
        var parent: Node = nodes.get(parent_id).getOrElse(new Node(parent_id))
        var child = nodes.get(child_id).getOrElse(new Node(child_id, parent_id))
        
        child.parent = parent.node_id
        parent.children = child :: parent.children 
        //parent.children += child
    
        nodes += (parent_id -> parent)
        nodes += (child_id -> child)
        
        return (parent, child)
    }
    
    def print_tree(node_id: Long = root.node_id, depth: Int = 0) {
        var children = nodes(node_id).children
        var total_children_area: Double = 0
        for (child <- children) 
            total_children_area += nodes(child.node_id).area
        println ("\t" * nodes(node_id).depth +  
            nodes(node_id).to_string() + 
            ", total_children_area: %f".format(total_children_area)
        )
        for (child <- children) print_tree(child.node_id, depth)
        
//        var current_generation = Queue[Long](root.node_id)
//        var next_generation = Queue[Long]()
//        var init = true
//        
//        while (!next_generation.isEmpty || init) {
//            init = false
//            next_generation.clear()
//            while (!current_generation.isEmpty) {
//                var node_id = current_generation.dequeue()
//                logging.info(
//                    "{0}, parent: {1}, depth: {2}, #children: {3}, " +
//                    "size: {4}, radius: {5}, area: {6}"
//                    .format(node_id,
//                            nodes(node_id).parent,
//                            nodes(node_id).depth,
//                            nodes(node_id).children.size,
//                            nodes(node_id).tree_size,
//                            nodes(node_id).radius,
//                            nodes(node_id).area))
//                for (child <- nodes(node_id).children) {
//                    next_generation += child.node_id
//                }
//            }
//            current_generation ++= (Seq() ++ next_generation)
//        }
    }
    
    def get_leaf_nodes() : Iterable[Long] = {
        for ((node_id, node) <- nodes; if node.children.isEmpty) 
          yield node.node_id
    }
    
    def label_node_generations(depthc: Int = 0) {
        var current_generation = Queue[Long](root.node_id)
        var next_generation = Queue[Long]()
        var depth: Int = depthc
        var init = true
        
        while (!next_generation.isEmpty || init) {
            init = false
            next_generation.clear()
            
            while (!current_generation.isEmpty) {
                var node_id = current_generation.dequeue()
                nodes(node_id).depth = depth
                for (child <- nodes(node_id).children) {
                  next_generation += child.node_id
                }
                val log_str = "next_generation of %d is %s, " +
                    " current_generation %s, depth %d"
                println(log_str.format(
                   node_id, next_generation, current_generation, depth))
            }
            
            depth += 1
            current_generation ++= (Seq() ++ next_generation)
        }
        
        height = depth - 1
    }
    
    def set_subtree_radius() {
        var leaf_nodes = get_leaf_nodes()
        var outermost_non_leaf: Set[Long] = Set()
        
        for (n <- leaf_nodes) {
            var N = nodes(nodes(n).parent).children.size
            nodes(n).radius = compute_radius(0.0025)
            //logging.debug("leaf node {0}, parent {1}, radius {2}"
            println("leaf node %d, parent %d, radius %f"
                         .format(n, nodes(n).parent, nodes(n).radius))
            outermost_non_leaf += nodes(n).parent
        }
        
        var depth: Int = height - 1
        var current_generation = Queue[Long]()
        for (node_id <- outermost_non_leaf) {
            if (nodes(node_id).parent != NULL_PARENT && 
                nodes(nodes(node_id).parent).depth == depth) {
                  current_generation += node_id
            }
        }
        
        var previous_generation = Queue[Long]()
        var init = true
        while (!previous_generation.isEmpty || init) {
            init = false
            previous_generation.clear()
            
            while (!current_generation.isEmpty) {
                var n = current_generation.dequeue()
                if (nodes(n).area == 0)  {// avoid duplicate parents
                    if (nodes(n).parent != NULL_PARENT) {
                        previous_generation += nodes(n).parent
                    }
                    for (child <- nodes(n).children) {
                        nodes(n).area += 7.2 * compute_hyperbolic_area(nodes(
                                child.node_id).radius)
                        val log_string = "node %d, child %d, child_area+ " +
                                         "%f, radius %f, area %f"
                        
                        println(log_string.format(n, child.node_id, 
                        //logging.debug(log_string.format(n, child, 
                                compute_hyperbolic_area(nodes(child.node_id)
                                    .radius),
                                nodes(child.node_id).radius, 
                                nodes(child.node_id).area))
                    }
                    nodes(n).radius = compute_radius(nodes(n).area)
                    println("---> node %d, radius %f, area %f"
                    //logging.debug("---> node {0}, radius {1}, area {2}"
                                 .format(n, nodes(n).radius, nodes(n).area))
                }
            }
            for (n <- outermost_non_leaf; if (nodes(n).depth == depth)) {
                previous_generation += n
            }
            depth -= 1
            current_generation ++= (Seq() ++ previous_generation.distinct)
        }
    }
    
    
    def set_subtree_size() {
        println("set subtree size")
        val leaf_nodes = get_leaf_nodes()
        var depth = height
        var current_generation = Queue[Long]()
        for (node_id <- leaf_nodes; if (nodes(node_id).depth == depth)) {
            current_generation += node_id
        }
        var previous_generation = Queue[Long]()
        var init = true
        while (!previous_generation.isEmpty || init) {
            init = false
            depth -= 1
            previous_generation.clear()
            
            while (!current_generation.isEmpty) {
                var n = current_generation.dequeue()
                if (nodes(n).parent != NULL_PARENT)  {
                    previous_generation += nodes(n).parent
                    nodes(nodes(n).parent).tree_size += nodes(n).tree_size
                    
                    println("%d size %d, parent %d size %d".format(n, 
                        nodes(n).tree_size, nodes(n).parent, 
                        nodes(nodes(n).parent).tree_size))
                }
            }
            for (n <- leaf_nodes; if (nodes(n).depth == depth)) {
                previous_generation += n
            }
            current_generation ++= (Seq() ++ previous_generation.distinct)
        }
    }
    
    def sort_children_by_radius() {
        println("set sort_children_by_radius")
        var depth = 0
        var current_generation = Queue[Long](root.node_id)
        var next_generation = Queue[Long]()
        var init = true
        
        while (!next_generation.isEmpty || init) {
            init = false
            next_generation .clear()
            while (!current_generation.isEmpty) {
                var node_id = current_generation.dequeue()
                for (child <- nodes(node_id).children) {
                    next_generation += child.node_id
                }
                var child_size_pair = nodes(node_id).children.toList.map { 
                    child => (child, nodes(child.node_id).radius)
                }
                child_size_pair = child_size_pair.sortBy(x=>(x._2))(Ordering
                    [Double].reverse)
                
                child_size_pair.map { c => 
                    println(node_id, c._1.node_id, c._2)
                }
                
                if (!child_size_pair.isEmpty) {
                  nodes(node_id).children = child_size_pair.map(p => 
                    p._1).distinct
//                  nodes(node_id).children = List[Node]()
//                  for (child <- (child_size_pair.map(p => p._1)).distinct)) {
//                    nodes(node_id).children = child :: nodes(node_id).children
//                  }
                }
            }
            depth += 1
            current_generation ++= (Seq() ++ next_generation.distinct)
        }
    }
    
    def set_placement() {
        println("set set_placement")
        var depth = 0
        var current_generation = Queue[Long]()
        current_generation ++= nodes(root.node_id).children.map(n => n.node_id)
                .toList
        var last_parent_id = root.node_id
        var next_generation = Queue[Long]()
        var init = true
        
        while (!next_generation.isEmpty || init) {
            init = false
            next_generation .clear()
            var (phi, theta, delta_theta, band) = (0.000001, 0.0, 0.0, 1)
            
            // span phi before jumping to the next band
            var last_max_phi: Double = 0  
            
            while (!current_generation.isEmpty) {
                var node = current_generation.dequeue()
                
                // same gen, diff parent
                if (nodes(node).parent != last_parent_id) {
                   last_parent_id = nodes(node).parent
                   var (phi, theta, delta_theta, band) = (0.000001, 0.0, 0.0, 1)
                }
                var rp = nodes(nodes(node).parent).radius
                try {
                    if (phi == 0.000001) {  // first child of root
                        phi += compute_delta_phi(nodes(node).radius, rp)
                        nodes(node).band = 0
                    }
                    else {
                        delta_theta = compute_delta_theta(nodes(node).radius, 
                            rp, phi)
                        if ((theta + delta_theta) <= 2 * Math.PI) {
                            theta += delta_theta
                            if (last_max_phi == 0) {
                                last_max_phi = compute_delta_phi(nodes(node)
                                    .radius, rp)
                                phi += compute_delta_phi(nodes(node).radius, rp)
                            }
                        }
                        else {
                            band += 1
                            theta = delta_theta
                            phi += last_max_phi + compute_delta_phi(nodes(node)
                                .radius, rp)
                            last_max_phi = 0
                        }
                        nodes(node).band = band
                        nodes(node).theta = theta
                        nodes(node).phi = phi
                    }
                }
                catch {
                    case e: ArithmeticException => 
                    val log_string = "%s\n node %d, radius=%f, rp=%f, " +
                            "phi=%f, parent=%d"
                    //logging.error(log_string.format(e, node, nodes(node).radius,
                    println(log_string.format(e, node, nodes(node).radius,
                        rp, phi, nodes(node).parent))
                }
                    
                nodes(node).coord.sph_to_cart(
                    nodes(node).theta,
                    nodes(node).phi,
                    nodes(nodes(node).parent).radius
                )
                
                if (nodes(node).parent != root.node_id) {
                  
                    nodes(node).coord.coordinate_transformation(
                        nodes(nodes(node).parent).theta,
                        nodes(nodes(node).parent).phi
                    )
                    nodes(node).coord.cart_offset(nodes(nodes(node).parent).coord)
                }
                //logging.debug("node {0}, radius {1},  band {2}, theta {3}, phi {4}"
//                println("node {0}, radius {1},  band {2}, theta {3}, phi {4}"
//                             .format(node, nodes(node).radius, nodes(node).band,
//                                     nodes(node).theta, nodes(node).phi))
//                
//                //logging.debug("node {0}, x {1}, y {2}, z {3}, w {4}"
//                println("node {0}, x {1}, y {2}, z {3}, w {4}"
//                             .format(node, nodes(node).coord.x, nodes(node).coord.y,
//                                     nodes(node).coord.z, nodes(node).coord.w))
                
                // reserve space for the other half sphere
                theta += delta_theta
                for (child <- nodes(node).children) {
                    next_generation += child.node_id
                }
            }
            depth += 1
            current_generation ++= (Seq() ++ next_generation)
        }
    }
  
}


