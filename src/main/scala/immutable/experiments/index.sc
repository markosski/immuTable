import scala.collection.mutable.Set
import scala.collection.mutable.HashMap

trait Field[A] {
    def convert(x: String): A // Need to define convert for the trait, too.
}

case class IntField extends Field[Int] {
    override def convert(x: String): Int = x.toInt
}

case class StringField extends Field[String] {
    override def convert(x: String): String = x
}

def someFunc[A](field: Field[A]): HashMap[A, Int] = {
    val index = new HashMap[A, Int]()
    val data = List("111", "222", "333")

    for (line <- data) {
        val values: A = field.convert(line)
        index.put(values, 0)
    }
    index
}
type A
//var fields = Set[Field[A] forSome {type A}]()
var fields = Set[Field[A]]()
def addField[A](field: Field[A]): Unit = fields += field

addField(StringField())
addField(IntField())

for (field <- fields) println(someFunc(field))

