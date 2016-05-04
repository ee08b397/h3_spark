package h3

import h3.h3math._

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
