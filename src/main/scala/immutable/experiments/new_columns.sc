import immutable.FixedCharColumn

trait Encoder {
    def loader(col: Column): Loader
    def iterator(col: Column): SeekableIterator[(Int, _)]

    trait SeekableIterator[A] extends Iterator[A] {
        def seek(pos: Int): Unit
        def next: A
        def hasNext: Boolean
    }

    trait Loader {
        def load(data: Vector[String]): Unit
        def finish: Unit
    }
}

trait NumericColumn {
    type DataType
    implicit val num: Numeric[DataType]
    val min: DataType
    val max: DataType
    val nullVal: DataType
}

trait CharColumn {
    val nullVal: Byte = 0
}

trait Column extends {
    type DataType
    type AltDataType
    implicit val ord: Ordering[DataType]

    val name: String
    val tblName: String
    val size: Int
    val nullRepr = "NULL"
    def stringToValue(s: String): DataType
    def stringToBytes(s: String): Array[Byte]
    def bytesToValue(bytes: Array[Byte]): DataType
    def validate(s: String): Unit
    def FQN = s"${tblName}.${name}"
}

abstract class FixedCharColumn(name: String, tblName: String, size: Int) extends Column with CharColumn {
    type DataType = String
    type AltDataType = Int
    val ord = implicitly[Ordering[DataType]]

    def stringToValue(s: String): String = s.slice(0, size)

    def stringToBytes(s: String): Array[Byte] = s.getBytes.slice(0, size).padTo(size, 0.toByte)

    def bytesToValue(bytes: Array[Byte]): String = new String(bytes.filter(_ != 0))

    def validate(s: String) = {
        if (s.length > size) {
        }
    }
}

val col = new {val name = "fname"; val tblName = "something"; val size = 1} extends FixedCharColumn