package immutable.helpers

/**
 * Created by marcin on 9/10/15.
 */

object Conversions {
    def bytesToInt(bytes: Array[Byte]): Int = bytes.reverse.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF))

    def intToBytes(i: Int, size: Int = 4): Array[Byte] = {
        (1 to size - 1).map(_ * 8).foldLeft(Array[Byte]((i & 0xFF).toByte))((b, a) => b ++ Array[Byte](((i >> a) & 0xFF).toByte))
    }

    def shortToBytes(i: Short, size: Int = 2): Array[Byte] = {
        (1 to size - 1).map(_ * 8).foldLeft(Array[Byte]((i & 0xFF).toByte))((b, a) => b ++ Array[Byte](((i >> a) & 0xFF).toByte))
    }

    def longToBytes(i: Long, size: Int = 8): Array[Byte] = {
        (1 to size - 1).map(_ * 8).foldLeft(Array[Byte]((i & 0xFF).toByte))((b, a) => b ++ Array[Byte](((i >> a) & 0xFF).toByte))
    }
}

