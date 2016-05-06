package h3

class DistinctList[A](list: List[A]) { 
  
    var values = list.distinct 
    
    def add(x: A) {
      values ::= x
    }
    
    implicit def toDistinctList[A](l: List[A]) = new DistinctList(l)
  
    implicit def fromDistinctList[A](l: DistinctList[A]) = l.values
    
}

