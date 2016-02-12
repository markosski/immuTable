
import scala.collection.mutable
import scala.collection.mutable.HashMap

trait Field
case class IntField extends Field {
    def convert(x: String) = x.toInt
}
case class StringField extends Field {
    def convert(x: String) = x
}

//def test(field: Field): HashMap[String, Int] = {
//    new mutable.HashMap[String, Int]() // This needs to be [String, Int] or [Int, Int]
//
//    val data = ??? // some data from csv file that will be parsed
//    val values = field.convert(data) // can return String or Int
//
//    values.put(values, 0)
//}

