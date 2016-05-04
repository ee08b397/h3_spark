package test.unit

import org.junit.Test
import org.junit.Assert.assertEquals

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import h3.Node

@RunWith(classOf[JUnitRunner])
class test_node extends FunSuite with BeforeAndAfter {
  
  test("test node ") {
    val n: Node = new Node(1, 2)
    
    assertEquals(n.node_id, 1)
    assertEquals(n.parent, 2)
  }
    
}