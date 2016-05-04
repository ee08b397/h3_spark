package test.unit

import org.junit.Test
import org.junit.Assert.assertEquals

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import h3.Point4d

@RunWith(classOf[JUnitRunner])
class test_point4d extends FunSuite with BeforeAndAfter {
  
  test("test point4d") {
    val p: Point4d = new Point4d(1.0, 2.0, 3.0, 4.0)
    assertEquals(p.x, 1.0, 0.01)
    assertEquals(p.y, 2.0, 0.01)
    assertEquals(p.z, 3.0, 0.01)
    assertEquals(p.w, 4.0, 0.01)
  }
    
}