
import scala.collection.mutable.MutableList
import scala.collection.mutable.HashMap

var l: MutableList[Int] = MutableList()

val index = new HashMap[Int, List[Int]]()

for (i <- 0 to 10) {
    val idxValue = index.getOrElse(i, List())
    val newIdxValue = idxValue ++ List(i)
    index.put(i, newIdxValue)
}

print(index)
