package h3

import h3.Point4d
import collection.mutable.ListBuffer
import Config.NULL_PARENT

class Node(val node_idc: Long, val parent_idc: Long = NULL_PARENT, 
        val depthc: Int = 0, val tree_sizec: Int = 1, val radiusc: Double = 0, 
        val areac: Double = 0) {

    var node_id: Long = node_idc
    var parent: Long = parent_idc
    
    //var children: ListBuffer[Node] = ListBuffer.empty[Node]
    var children: List[Node] = List[Node]()
    var depth: Int = depthc
    var tree_size: Int = tree_sizec
    var radius: Double = radiusc
    var area: Double = areac
    var band: Int = -1
    var theta: Double = 0
    var phi: Double = 0
    var coord: Point4d = new Point4d()
    
    def to_string():String = {
        return s"$node_id, " + 
               s"parent: $parent, " +
               s"children: " + children.map { n => n.node_id } + ", " +
               s"depth: $depth, " +
               s"tree_size: $tree_size, " +
               f"radius: $radius, " + 
               s"area: $area, " + 
               s"band: $band, " + 
               s"theta: $theta, " + 
               s"phi: $phi, " + 
               "coord: " + coord.to_string()
    }
    
}