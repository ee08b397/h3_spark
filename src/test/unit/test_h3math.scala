package test.unit

import org.junit.Test
import org.junit.Assert.assertEquals

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import h3.h3math._

@RunWith(classOf[JUnitRunner])
class h3math_test extends FunSuite with BeforeAndAfter {
  
  test("test matrix multiplication ") {
    val A : Matrix = List(
      List( 2.0, 0.0 ),
      List( 3.0,-1.0 ),
      List( 0.0, 1.0 ),
      List( 1.0, 1.0 ))
    val B : Matrix = List(
      List( 1.0,  0.0, 2.0 ),
      List( 4.0, -1.0, 0.0 ))
    val C : Matrix = List(List(  2.0,  0.0, 4.0 ),
      List( -1.0,  1.0, 6.0 ),
      List(  4.0, -1.0, 0.0 ),
      List(  5.0, -1.0, 2.0 ))
    val D = A * B
    assertEquals(D, C)
    //println(D.map(f => println(f.mkString(","))))
    
    val a = List(
        List(1.0,2.0,3.0), 
        List(4.0,5.0,6.0), 
        List(7.0,8.0,9.0))
    val b = List(
        List(30.0,36.0,42.0), 
        List(66.0,81.0,96.0),
        List(102.0,126.0,150.0))
    assertEquals(a * a, b)
    //println((a * b).map(f => println(f.mkString(","))))
  }
    
}