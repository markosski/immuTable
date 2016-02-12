
//trait A {
//    val name: String
//}
//
//trait CanCopy[T <: CanCopy[T]] { def copy(newName: String): T }
//
//case class B(name: String) extends A with CanCopy[B] {
//    def copy(newName: String) = B(newName)
//}
//
//def newALike[T <: A with CanCopy[T]](instance: T): T = instance.copy(name = instance.name + "_new")



abstract class A[T <: A[T]] {
    val name: String
    def make(newName: String): T
}

case class B(name: String) extends A[B] {
    def make(newName: String): B = B(newName)
}

val b = B("B")

def makeNewInstance(b: A[B]) = {
    b.make("newest B")
}

makeNewInstance(b)
