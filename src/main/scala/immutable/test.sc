abstract class Column[A] {
    val name: String
    val size: Int
    def stringToValue(s: String): A
}

case class IntColumn(name: String) extends Column[Int] {
    val size = 4
    def stringToValue(s: String): Int = s.toInt
}

val col = IntColumn("id")

import scala.collection.mutable.HashMap

def getMinFromHashMap[A: Ordering](col: Column[A]): (A, Int) = {
    val hm = HashMap[A, Int]()
    hm.put(col.stringToValue("1"), 1)
    hm.put(col.stringToValue("2"), 1)
    hm.put(col.stringToValue("3"), 1)
    hm.min
}

val min = getMinFromHashMap(col)

//def getHashMap[A](col: Column[A]): HashMap[A, Int] = HashMap[A, Int]()
//val hm = getHashMap(col)
//hm.put(col.stringToValue("1"), 1)
//hm.put(col.stringToValue("2"), 1)
//hm.put(col.stringToValue("3"), 1)
//hm.min

