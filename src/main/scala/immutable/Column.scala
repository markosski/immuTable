package immutable

import immutable.helpers.Conversions

import immutable.LoggerHelper._
import scala.reflect.runtime.universe._
import scala.Symbol

/**
 * http://ktoso.github.io/scala-types-of-types/
 *
 */

sealed trait NumericColumn {
    val min: AnyVal
    val max: AnyVal
    val nullVal: AnyVal
}

sealed trait CharColumn {
    val nullVal: Byte = 0
}

abstract class Column[@specialized(Byte, Short, Int) A]
    (implicit val ord: Ordering[A], implicit val tag: TypeTag[A]) {

    val name: String
    val size: Int
    val encoder: Symbol
    val nullRepr = "Null"
    def stringToValue(s: String): A
    def stringToBytes(s: String): Array[Byte]
    def bytesToValue(bytes: Array[Byte]): A
    def validate(s: String): Unit
}

object ColumnImplicits {
    implicit class VarCharToIntColumn(col: VarCharColumn) {
        def toIntColumn = IntColumn(col.name)
    }
}


case class FixedCharColumn(name: String, size: Int, encoder: Symbol='Dense)
        extends Column[String] with CharColumn {

    def stringToValue(s: String): String = s.slice(0, size)

    def stringToBytes(s: String): Array[Byte] = s.getBytes.slice(0, size).padTo(size, 0.toByte)

    def bytesToValue(bytes: Array[Byte]): String = new String(bytes.filter(_ != 0))

    def validate(s: String) = {
        if (s.length > size) {
            warn(f"Truncating value of column $name%s to $size%d.")
        }
    }
}

case class VarCharColumn(name: String, size: Int, encoder: Symbol='Dense)
        extends Column[String] with CharColumn {

    def stringToValue(s: String): String = s.slice(0, size)

    def stringToBytes(s: String): Array[Byte] = s.getBytes.slice(0, size)

    def bytesToValue(bytes: Array[Byte]): String = new String(bytes.filter(_ != 0))

    def validate(s: String) = {
        if (s.length > size) {
            warn(f"Truncating value of column $name%s to $size%d.")
        }
    }
}

case class DateColumn(name: String, encoder: Symbol='Dense)
        extends Column[String] with CharColumn {
    /**
      * year: 0-9999 - 2 bytes
      * month: 1-12 - 1 byte
      * day: 1-31 - 1 byte
      * hour: 0-23 - 1 byte
      * minute: 0-59 - 1 byte
      * second: 0-59 - 1 byte
      *
      * Accepted formats:
      * 9999-12-31
      * 9999-12-31 00:00:00
      *
      * @return
      */
    val size = 7
    def stringToValue(x: String): String = x
    def stringToBytes(x: String): Array[Byte] = {
        def parseDate(date: String): Array[Byte] = {
            val parts = date.split('-')
            Conversions.shortToBytes(parts(0).toShort) :+ parts(1).toByte :+ parts(2).toByte
        }

        def parseTime(time: String): Array[Byte] = {
            val parts = time.split(':')
            Array[Byte](parts(0)toByte, parts(1).toByte, parts(2).toByte)
        }

        var date = Array[Byte]()
        var time = Array[Byte]()
        if (x.split(' ').length == 2) {
            date = parseDate(x.split(' ')(0))
            time = parseTime(x.split(' ')(1))
        } else {
            date = parseDate(x.split(' ')(0))
            time = Array[Byte](0.toByte, 0.toByte, 0.toByte)
        }
        date ++ time
    }
    def bytesToValue(bytes: Array[Byte]): String = ???

    def validate(s: String) = {
        val parts = s.split('-')
        if (parts.length != 3) {
            throw new IllegalArgumentException("Wrong time format")
        }
    }
}

case class TinyIntColumn(name: String, encoder: Symbol='Dense)
        extends Column[Byte] with NumericColumn {
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

case class ShortIntColumn(name: String, encoder: Symbol='Dense)
        extends Column[Short] with NumericColumn {
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

case class IntColumn(name: String, encoder: Symbol='Dense)
        extends Column[Int] with NumericColumn {
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

case class DecimalColumn(name: String, encoder: Symbol='Dense)
        extends Column[Double] with NumericColumn {
    /**
     * http://stackoverflow.com/questions/9810010/scala-library-to-convert-numbers-int-long-double-to-from-arraybyte
     */
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

