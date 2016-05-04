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