package immutable

import immutable.encoders.{Encoder}
import immutable.LoggerHelper._
import immutable.helpers.Conversions

/**
 * http://ktoso.github.io/scala-types-of-types/
 */

trait Column {
    type DataType
    type AltDataType
    implicit val ord: Ordering[DataType]

    val name: String
    val tblName: String
    val size: Int
    val enc: Encoder
    val nullRepr = "null"
    def stringToValue(s: String): DataType
    def stringToBytes(s: String): Array[Byte]
    def bytesToValue(bytes: Array[Byte]): DataType
    def validate(s: String): Unit
    def getIterator = enc.iterator(this)
    def getLoader = enc.loader(this)
    def FQN = s"${tblName}.${name}"
}

trait NumericColumn extends Column {
    type DataType
    implicit val num: Numeric[DataType]
    val min: DataType
    val max: DataType
    val nullVal: DataType
}

trait CharColumn extends Column {
    val nullVal: Array[Byte]
}

case class FixedCharColumn(name: String, tblName: String, size: Int, enc: Encoder) extends CharColumn {
    type DataType = String
    type AltDataType = Int
    val ord = implicitly[Ordering[DataType]]
    val nullVal = new Array[Byte](size)

    def stringToValue(s: String): String = s match {
        case "null" => bytesToValue(nullVal)
        case _ => s.slice(0, size)
    }

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
    val nullVal = new Array[Byte](size)

    def stringToValue(s: String): String = s match {
        case "null" => bytesToValue(nullVal)
        case _ => s.slice(0, size)
    }

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

    def stringToValue(s: String): Byte = s match {
        case "null" => nullVal
        case _ => s.toByte
    }

    def stringToBytes(s: String): Array[Byte] = Array[Byte](s.toByte)

    def bytesToValue(bytes: Array[Byte]): Byte = bytes(0)

    def validate(s: String) = {
        if (s.toLong > Byte.MaxValue || s.toLong < Byte.MinValue + 1)
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

    def stringToValue(s: String): Short = s match {
        case "null" => nullVal
        case _ => s.toShort
    }

    def stringToBytes(s: String): Array[Byte] = {
        val stringAsValue = s.toShort
        val bytes = new Array[Byte](2)
        bytes(1) = ((stringAsValue >> 8) & 0xFF).toByte
        bytes(0) = (stringAsValue & 0xFF).toByte
        bytes
    }

    def bytesToValue(bytes: Array[Byte]): Short = {
        Conversions.bytesToShort(bytes)
    }

    def validate(s: String) = {
        if (s.toLong > Short.MaxValue || s.toLong < Short.MinValue + 1)
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

    def stringToValue(s: String): Int = s match {
        case "null" => nullVal
        case _ => s.toInt
    }

    def stringToBytes(s: String): Array[Byte] = {
        val stringAsValue = s.toInt
        val bytes = new Array[Byte](4)
        bytes(3) = ((stringAsValue >> 24) & 0xFF).toByte
        bytes(2) = ((stringAsValue >> 16) & 0xFF).toByte
        bytes(1) = ((stringAsValue >> 8) & 0xFF).toByte
        bytes(0) = (stringAsValue & 0xFF).toByte
        bytes
    }

    def bytesToValue(bytes: Array[Byte]): Int = {
        Conversions.bytesToInt(bytes)
    }

    def validate(s: String) = {
        if (s.toLong > Int.MaxValue || s.toLong < Int.MinValue + 1)
            throw new IllegalArgumentException(f"Value $s%s is out of bound for data type IntColumn.")
    }
}

case class BigIntColumn(name: String, tblName: String, enc: Encoder) extends NumericColumn {
    type DataType = Long
    val ord = implicitly[Ordering[DataType]]
    val num = implicitly[Numeric[DataType]]

    val nullVal = Long.MinValue
    val min = Long.MinValue + 1
    val max = Long.MaxValue
    val size = 8

    def stringToValue(s: String): Long = s.toLong

    def stringToBytes(s: String): Array[Byte] = {
        val stringAsValue = s.toInt
        val bytes = new Array[Byte](4)
        bytes(7) = ((stringAsValue >> 56) & 0xFF).toByte
        bytes(6) = ((stringAsValue >> 48) & 0xFF).toByte
        bytes(5) = ((stringAsValue >> 40) & 0xFF).toByte
        bytes(4) = ((stringAsValue >> 32) & 0xFF).toByte
        bytes(3) = ((stringAsValue >> 24) & 0xFF).toByte
        bytes(2) = ((stringAsValue >> 16) & 0xFF).toByte
        bytes(1) = ((stringAsValue >> 8) & 0xFF).toByte
        bytes(0) = (stringAsValue & 0xFF).toByte
        bytes
    }

    def bytesToValue(bytes: Array[Byte]): Long = {
        Conversions.bytesToLong(bytes)
    }

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
    def stringToValue(s: String) = s match {
        case "null" => nullVal
        case _ => s.toDouble
    }

    def bytesToValue(bytes: Array[Byte]) = {
        var i = 0
        var res = 0.toLong
        for (i <- 0 to 7) res += ((bytes(i) & 0xff).toLong << ((7 - i) * 8))
        java.lang.Double.longBitsToDouble(res)
    }

    def validate(s: String) = {}
}

