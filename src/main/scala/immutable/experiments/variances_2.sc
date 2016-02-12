import scala.collection.mutable.Set
import scala.collection.mutable.HashMap

// classes to create field types that do conversion of string to other types

trait Field[A] {
    def convert(x: String): A // Need to define convert for the trait, too.
}

case class IntField extends Field[Int] {
    override def convert(x: String): Int = x.toInt
}

case class StringField extends Field[String] {
    override def convert(x: String): String = x
}

// this function can take any Field type and return a HashMap,
// more important here is type of key not the value of HashMap
// which has to match with value returned from Field.convert()

def someFunc[A](field: Field[A]): HashMap[A, Int] = {
    val index = new HashMap[A, Int]()
    val data = List("111", "222", "333")

    for (line <- data) {
        val values: A = field.convert(line)
        index.put(values, 0)
    }
    index
}

// this empty set will be populated with Field objects, and here I get an error
var fields = Set[Field[A] forSome {type A}]()

def addField[A](field: Field[A]): Unit = fields += field

addField(StringField())
addField(IntField())

for (field <- fields) println(someFunc(field))
// RESULT
