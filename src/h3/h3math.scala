package h3

/* 
 * H3 math models
 */
object h3math {
  
  type Row = List[Double]
  type Matrix = List[Row]
  
  
  def dotProd(v1 : Row, v2 : Row) = 
      v1.zip(v2).map{
          t:(Double, Double) => t._1 * t._2
      }.reduceLeft(_ + _)
  
  def transpose(m : Matrix) : Matrix =
      if(m.head.isEmpty)
        Nil
      else 
        m.map(_.head) :: transpose(m.map(_.tail))
  
  def mXm(m1 : Matrix, m2 : Matrix) =
      for(m1row <- m1 ) yield
          for(m2col <- transpose(m2)) yield
              dotProd(m1row, m2col)
  
  case class RichMatrix(m:Matrix){
    def T = transpose(m)
    def *(that:RichMatrix) = mXm( this.m, that.m )
  }

  implicit def pimp(m:Matrix) = new RichMatrix(m)
 
  def rotation_matrix_x(angle : Double) : Matrix = {
      return List(List(1, 0, 0, 0),
                  List(0, math.cos(angle), -1 * math.sin(angle), 0),
                  List(0, math.sin(angle), math.cos(angle), 0),
                  List(0, 0, 0, 1))
      }
  
  def rotation_matrix_y(angle : Double) : Matrix = {
      return List(List(math.cos(angle), 0, math.sin(angle), 0),
                  List(0, 1, 0, 0),
                  List(-1 * math.sin(angle), 0, math.cos(angle), 0),
                  List(0, 0, 0, 1))
      }
  
  def rotation_matrix_z(angle : Double) : Matrix = {
      return List(List(math.cos(angle), -1 * math.sin(angle), 0, 0),
                  List(math.sin(angle), math.cos(angle), 0, 0),
                  List(0, 0, 1, 0),
                  List(0, 0, 0, 1))
      }
  
  
  class Point4d(val xc: Double = 0.0, val yc: Double = 0.0, val zc: 
          Double = 0.0, val wc: Double = 1.0) {
      
      var x : Double = xc
      var y : Double = yc
      var z : Double = zc
      var w : Double = wc
  
      def sph_to_cart(theta : Double, phi : Double, r : Double) {
          x = r * math.sin(phi) * math.cos(theta)
          y = r * math.sin(phi) * math.sin(theta)
          z = r * math.cos(phi)
      }
  
      def cart_offset(offset : Point4d) {
          x += offset.x
          y += offset.y
          z += offset.z
      }
  
      def translate(offset : Point4d) {
          val translation_matrix = List(List(1.0, 0.0, 0.0, offset.x),
                                        List(0.0, 1.0, 0.0, offset.y),
                                        List(0.0, 0.0, 1.0, offset.z),
                                        List(0.0, 0.0, 0.0, 1.0))
          val target = translation_matrix * List(List(x, y, z, w))
          x = target(0)(0)
          y = target(0)(1)
          z = target(0)(2)
          w = target(0)(3)
      }
  
      def coordinate_transformation(theta : Double, phi : Double) {
          val rotation_matrix = rotation_matrix_z(theta) * 
                rotation_matrix_y(phi)
          val target = rotation_matrix * List(List(x, y, z, w))
          x = target(0)(0)
          y = target(0)(1)
          z = target(0)(2)
          w = target(0)(3)
      }
  }
  
  
  val K : Double = 2.0
  
  def asinh(x : Double) : Double = {
      return math.log(x + math.sqrt(1.0 + x * x))
  }
  
  def acosh(x : Double) : Double = {
      return math.log(x + math.sqrt(x * x - 1.0))
  }
  
  def compute_radius(H_p : Double) : Double = {
      return K * asinh(math.sqrt(H_p / (2 * math.Pi * K * K)))
  }
  
  def compute_hyperbolic_area(radius : Double) : Double = {
      val beta = 1.00
      return 2 * math.Pi * (math.cosh(radius / K) - 1.0) * beta
  }
  
  def compute_delta_theta(r : Double, rp : Double, phi : Double) : Double = {
      return math.atan(math.tanh(r / K) /
                       (math.sinh(rp / K) * math.sinh(phi)))
  }
  
  def compute_delta_phi(r : Double, rp : Double) : Double = {
      return math.atan(math.tanh(r / K) / math.sinh(rp / K))
  }
  
  def minkowski(x : Point4d, y : Point4d) : Double = {
      return (x.x * y.x + x.y * y.y + x.z * y.z - x.w * y.w)
  }
  
  def hyperbolic_distance(x : Point4d, y : Point4d) : Double = {
      val t1 : Double = minkowski(x, y)
      val t2 : Double = minkowski(x, x)
      val t3 : Double = minkowski(y, y)
      return (2 * acosh(math.pow(((t1 * t1) / (t2 * t3)), 2)))
  }

}