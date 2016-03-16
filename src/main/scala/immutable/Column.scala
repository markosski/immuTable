package immutable

import immutable.encoders.{Encoder}
import immutable.LoggerHelper._

/**
 * http://ktoso.github.io/scala-types-of-types/
 */

trait NumericColumn {
    type DataType
    val min: DataType
    val max: DataType
    val nullVal: DataType
}

trait CharColumn {
    val nullVal: Byte = 0
}

trait Column {
    type DataType
    implicit val ord: Ordering[DataType]

    val name: String
    val tblName: String
    val size: Int
    val enc: Encoder
    val nullRepr = "NULL"
    def stringToValue(s: String): DataType
    def stringToBytes(s: String): Array[Byte]
    def bytesToValue(bytes: Array[Byte]): DataType
    def validate(s: String): Unit
    def getIterator = enc.iterator(this)
    def getLoader = enc.loader(this)
    def FQN = s"${tblName}.${name}"
}

case class FixedCharColumn(name: String, tblName: String, size: Int, enc: Encoder) extends Column with CharColumn {
    type DataType = String
    val ord = implicitly[Ordering[DataType]]

    def stringToValue(s: String): String = s.slice(0, size)

    def stringToBytes(s: String): Array[Byte] = s.getBytes.slice(0, size).padTo(size, 0.toByte)

    def bytesToValue(bytes: Array[Byte]): String = new String(bytes.filter(_ != 0))

    def validate(s: String) = {
        if (s.length > size) {
            warn(f"Truncating value of column $name%s to $size%d.")
        }
    }
}

case class VarCharColumn(name: String, tblName: String, size: Int, enc: Encoder)
        extends Column with CharColumn {
    type DataType = String
    val ord = implicitly[Ordering[DataType]]

    def stringToValue(s: String): String = s.slice(0, size)

    def stringToBytes(s: String): Array[Byte] = s.getBytes.slice(0, size)

    def bytesToValue(bytes: Array[Byte]): String = new String(bytes.filter(_ != 0))

    def validate(s: String) = {
        if (s.length > size) {
            warn(f"Truncating value of column $name%s to $size%d.")
        }
    }
}


case class TinyIntColumn(name: String, tblName: String, enc: Encoder)
        extends Column with NumericColumn {
    type DataType = Byte
    val ord = implicitly[Ordering[DataType]]


    val nullVal = Byte.MinValue
    val min = (Byte.MinValue + 1).toByte
    val max = Byte.MaxValue
    val size = 1

    def stringToValue(s: String): Byte = s.toByte

    def stringToBytes(s: String): Array[Byte] = Array[Byte](s.toByte)

    def bytesToValue(bytes: Array[Byte]): Byte = bytes(0)

    def validate(s: String) = {
        if (s.toLong > Byte.MaxValue || s.toLong < Byte.MinValue + 1)
            throw new IllegalArgumentException(f"Value $s%s is out of bound for data type TinyIntColumn.")
    }
}

case class ShortIntColumn(name: String, tblName: String, enc: Encoder)
        extends Column with NumericColumn {
    type DataType = Short
    val ord = implicitly[Ordering[DataType]]

    val nullVal = Short.MinValue
    val min = (Short.MinValue + 1).toShort
    val max = Short.MaxValue
    val size = 2

    def stringToValue(s: String): Short = s.toShort

    def stringToBytes(s: String): Array[Byte] = {
        List(8).foldLeft(Array[Byte]((stringToValue(s) & 0xFF).toByte))((b, a) => b ++ Array[Byte](((stringToValue(s) >> a) & 0xFF).toByte))
    }

    def bytesToValue(bytes: Array[Byte]): Short = bytes.reverse.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF)).toShort

    def validate(s: String) = {
        if (s.toLong > Short.MaxValue || s.toLong < Short.MinValue + 1)
            throw new IllegalArgumentException(f"Value $s%s is out of bound for data type ShortColumn.")
    }
}

case class IntColumn(name: String, tblName: String, enc: Encoder)
        extends Column with NumericColumn {
    type DataType = Int
    val ord = implicitly[Ordering[DataType]]

    val nullVal = Int.MinValue
    val min = Int.MinValue + 1
    val max = Int.MaxValue
    val size = 4

    def stringToValue(s: String): Int = s.toInt

    def stringToBytes(s: String): Array[Byte] = {
        List(8, 16, 24).foldLeft(Array[Byte]((stringToValue(s) & 0xFF).toByte))((b, a) => b ++ Array[Byte](((stringToValue(s) >> a) & 0xFF).toByte))
    }

    def bytesToValue(bytes: Array[Byte]): Int = bytes.reverse.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF))

    def validate(s: String) = {
        if (s.toLong > Int.MaxValue || s.toLong < Int.MinValue + 1)
            throw new IllegalArgumentException(f"Value $s%s is out of bound for data type IntColumn.")
    }
}

case class DecimalColumn(name: String, tblName: String, enc: Encoder)
        extends Column with NumericColumn {
    /**
     * http://stackoverflow.com/questions/9810010/scala-library-to-convert-numbers-int-long-double-to-from-arraybyte
     */
    type DataType = Double
    val ord = implicitly[Ordering[DataType]]

    val nullVal = Int.MinValue.toDouble
    val min = Int.MinValue.toDouble + 1
    val max = Int.MaxValue.toDouble
    val size = 8

    def stringToBytes(s: String) = {
        val l = java.lang.Double.doubleToLongBits(s.toDouble)
        val a = Array.fill(8)(0.toByte)
        for (i <- 0 to 7) a(i) = ((l >> ((7 - i) * 8)) & 0xff).toByte
        a
    }
    def stringToValue(s: String) = s.toDouble

    def bytesToValue(bytes: Array[Byte]) = {
        var i = 0
        var res = 0.toLong
        for (i <- 0 to 7) res += ((bytes(i) & 0xff).toLong << ((7 - i) * 8))
        java.lang.Double.longBitsToDouble(res)
    }

    def validate(s: String) = {}
}

