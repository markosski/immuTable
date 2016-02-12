import scala.collection.mutable.HashMap

trait Column[A] { val name: String }
case class IntColumn(name: String) extends Column[Int]
case class StringColumn(name: String) extends Column[String]

var indexes = Map[String, HashMap[A forSome {type A}, Array[Int]]]()

def createIndex[A](column: Column[A]): HashMap[A forSome {type A}, Array[Int]] = {
    val index = HashMap[A forSome {type A}, Array[Int]]()

    // parsing raw text files and converting data to either Int or String
    // depending on kind of column:
    // index.put("somestring", Array(1, 2, 3))
    // or
    // index.put(120, Array(1,2,3))

    indexes += (column.name -> index)
    index
}

def getOrCreate[A](column: Column[A]): HashMap[A forSome {type A}, Array[Int]] = {
    if (indexes.contains(column.name)) {
        indexes.get(column.name).get
    } else {
        createIndex(column)
    }
}

def search[A](column: Column[A], value: A): Array[Int] = {
    val index = getOrCreate(column)
    index.get(value).get
}

val stringCol = StringColumn("name")
val intCol = IntColumn("age")

createIndex(stringCol)
createIndex(intCol)

// search(charCol, "Some val") -> should return Array(1, 2, 3)




