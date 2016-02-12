

//def foo[T <: AnyVal](x : Array[T]) = println(x.toString)
//foo(Array[String]("foo", "bar", "baz"))
//foo(Array[Double](1,2,3))

def foo(x: Array[Any]) = println(x.length)

foo(Array[Any]("foo", "baz", 1))
