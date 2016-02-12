
import Numeric.Implicits._
//import Numeric._

object Compare {
    def gt[A: Numeric](a: A, b: A): Boolean = {
        if (implicitly[Numeric[A]].gt(a, b)) true
        else false
    }
}

//case class Thing[A:Numeric](a: A, b: A) {
//    def compare[A:Numeric](a: A, b: A): Boolean = {
//        if (implicitly[Numeric[A]].gt(a, b)) true
//        else false
//
//    }
//    def run = {
//        if (compare(a, b)) println("Yay!")
//        else println("Nay :(")
//    }
//}

//Thing(0, 1).run
Compare.gt(5, 1)