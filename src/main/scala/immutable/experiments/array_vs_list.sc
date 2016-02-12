
var a = List[Int](); for (i <- 0 to 20000) a = a ++ List(i)
a.size
var b = Array[Int](); for (i <- 0 to 20000) b = b ++ Array(i)
b.size