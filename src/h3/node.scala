package h3

import h3.Point4d

class Node(val node_idc: Int, val parent_idc: Int, val depthc: Int = 0, 
    val tree_sizec: Int = 1, val radiusc: Int = 0, val areac: Int = 0){

  val node_id: Int = node_idc
  val parent: Int = parent_idc
  
  var children = Set()
  var depth: Int = depthc
  var tree_size: Int = tree_sizec
  var radius: Int = radiusc
  var area: Int = areac
  var band: Int = -1
  var theta: Int = 0
  var phi: Int = 0
  var coord: Point4d = new Point4d(0.0, 0.0, 0.0, 0.0)
  
}