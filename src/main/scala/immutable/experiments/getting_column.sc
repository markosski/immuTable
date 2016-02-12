
/**
 * http://stackoverflow.com/questions/15186520/scala-any-vs-underscore-in-generics
 *
 * http://like-a-boss.net/2012/09/17/variance-in-scala.html
 *
 */

// Why traits can be used instead of abstract classes
trait Column[+A] {
    val name: String
}

case class IntColumn(name: String) extends Column[Int] {
    val value = 0
}
case class StringColumn(name: String) extends Column[String] {
    val value = "empty"
}

val columns = List(IntColumn("int_column"), StringColumn("string_column"))

def getColumn[B](cols: List[Column[Any]], col_name: String): B = {
    cols.find(_.name == col_name).get match {
        case x: B => x
        case _ => throw new Exception("Column Not found")
    }
}

val col = getColumn[IntColumn](columns, "int_column")
col.value

