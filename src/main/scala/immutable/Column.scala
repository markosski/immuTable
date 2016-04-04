package immutable

import immutable.encoders.{Encoder}
import immutable.LoggerHelper._

/**
 * http://ktoso.github.io/scala-types-of-types/
 */

trait NumericColumn extends Column {
    type DataType
    implicit val num: Numeric[DataType]
    val min: DataType
    val max: DataType
//    val nullVal: DataType
}

trait CharColumn extends Column {
//    val nullVal: Byte = 0
}

trait Column extends {
    type DataType
    type AltDataType
    implicit val ord: Ordering[DataType]

    val name: String
    val tblName: String
    val size: Int
    val enc: Encoder
    val nullVal: DataType
    val nullRepr = "null"
    def stringToValue(s: String): DataType
    def stringToBytes(s: String): Array[Byte]
    def bytesToValue(bytes: Array[Byte]): DataType
    def validate(s: String): Unit
    def getIterator = enc.iterator(this)
    def getLoader = enc.loader(this)
    def FQN = s"${tblName}.${name}"
}

case class FixedCharColumn(name: String, tblName: String, size: Int, enc: Encoder) extends CharColumn {
    type DataType = String
    type AltDataType = Int
    val ord = implicitly[Ordering[DataType]]
    val nullVal = "\0"

    def stringToValue(s: String): String = s.slice(0, size)

    def stringToBytes(s: String): Array[Byte] = s.getBytes.slice(0, size).padTo(size, 0.toByte)

    def bytesToValue(bytes: Array[Byte]): String = new String(bytes.filter(_ != 0))

    def validate(s: String) = {
        if (s.length > size) {
            warn(f"Truncating value of column $name%s to $size%d.")
        }
    }
}

case class VarCharColumn(name: String, tblName: String, size: Int, enc: Encoder) extends CharColumn {
    type DataType = String
    type AltDataType = Int
    val ord = implicitly[Ordering[DataType]]
    val nullVal = "\0"

    def stringToValue(s: String): String = s.slice(0, size)

    def stringToBytes(s: String): Array[Byte] = s.getBytes.slice(0, size)

    def bytesToValue(bytes: Array[Byte]): String = new String(bytes.filter(_ != 0))

    def validate(s: String) = {
        if (s.length > size) {
            warn(f"Truncating value of column $name%s to $size%d.")
        }
    }
}


case class TinyIntColumn(name: String, tblName: String, enc: Encoder) extends NumericColumn {
    type DataType = Byte
    type AltDataType = DataType
    val ord = implicitly[Ordering[DataType]]
    val num = implicitly[Numeric[DataType]]


    val nullVal = Byte.MinValue
    val min = (Byte.MinValue + 1).toByte
    val max = Byte.MaxValue.toByte
    val size = 1

    def stringToValue(s: String): Byte = s.toByte

    def stringToBytes(s: String): Array[Byte] = Array[Byte](s.toByte)

    def bytesToValue(bytes: Array[Byte]): Byte = bytes(0)

    def validate(s: String) = {
        if (s.toLong > max || s.toLong < min)
            throw new IllegalArgumentException(f"Value $s%s is out of bound for data type TinyIntColumn.")
    }
}

case class ShortIntColumn(name: String, tblName: String, enc: Encoder) extends NumericColumn {
    type DataType = Short
    type AltDataType = DataType
    val ord = implicitly[Ordering[DataType]]
    val num = implicitly[Numeric[DataType]]

    val nullVal = Short.MinValue
    val min = (Short.MinValue + 1).toShort
    val max = Short.MaxValue.toShort
    val size = 2

    def stringToValue(s: String): Short = s.toShort

    def stringToBytes(s: String): Array[Byte] = {
        List(8).foldLeft(Array[Byte]((stringToValue(s) & 0xFF).toByte))((b, a) => b ++ Array[Byte](((stringToValue(s) >> a) & 0xFF).toByte))
    }

    def bytesToValue(bytes: Array[Byte]): Short = bytes.reverse.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF)).toShort

    def validate(s: String) = {
        if (s.toLong > max || s.toLong < min)
            throw new IllegalArgumentException(f"Value $s%s is out of bound for data type ShortColumn.")
    }
}

case class IntColumn(name: String, tblName: String, enc: Encoder) extends NumericColumn {
    type DataType = Int
    type AltDataType = DataType
    val ord = implicitly[Ordering[DataType]]
    val num = implicitly[Numeric[DataType]]

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
        if (s.toLong > max || s.toLong < min)
            throw new IllegalArgumentException(f"Value $s%s is out of bound for data type IntColumn.")
    }
}

case class BigIntColumn(name: String, tblName: String, enc: Encoder) extends NumericColumn {
    type DataType = Long
    type AltDataType = DataType
    val ord = implicitly[Ordering[DataType]]
    val num = implicitly[Numeric[DataType]]

    val nullVal = Long.MinValue
    val min = Long.MinValue + 1
    val max = Long.MaxValue
    val size = 8

    def stringToValue(s: String): Long = s.toLong

    def stringToBytes(s: String): Array[Byte] = {
        List(8, 16, 24, 32, 40, 48, 56).foldLeft(Array[Byte]((stringToValue(s) & 0xFF).toByte))((b, a) => b ++ Array[Byte](((stringToValue(s) >> a) & 0xFF).toByte))
    }

    def bytesToValue(bytes: Array[Byte]): Long = bytes.reverse.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF))

    def validate(s: String) = {
        if (s.toLong > max || s.toLong < min)
            throw new IllegalArgumentException(f"Value $s%s is out of bound for data type IntColumn.")
    }
}

case class DecimalColumn(name: String, tblName: String, enc: Encoder) extends NumericColumn {
    /**
     * http://stackoverflow.com/questions/9810010/scala-library-to-convert-numbers-int-long-double-to-from-arraybyte
     */
    type DataType = Double
    type AltDataType = DataType
    val ord = implicitly[Ordering[DataType]]
    val num = implicitly[Numeric[DataType]]

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

