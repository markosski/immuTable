package experiments

import java.util.Date
import scala.util.Random

/**
 * Created by marcin on 12/8/15.
 */
object RunLengthEncoding extends App {
    def genData: Array[Array[Int]] = {
        val size = 4000000
        val a = new Array[Array[Int]](size)

        for (i <- 0 until size) {
            a(i) = Array[Int](i, Random.nextInt(100))
        }
        a
    }

    def genByteData: Array[Byte] = {
        val size = 10000000
        val a = new Array[Byte](size*8)
        val random = Random.nextInt(100)

        for (i <- 0 until size*8) {
            a(i) = (i >> 0 & 0xFF).toByte
            a(i+1) = (i >> 8 & 0xFF).toByte
            a(i+2) = (i >> 16 & 0xFF).toByte
            a(i+3) = (i >> 24 & 0xFF).toByte
            a(i+4) = (random >> 0 & 0xFF).toByte
            a(i+5) = (random >> 8 & 0xFF).toByte
            a(i+6) = (random >> 16 & 0xFF).toByte
            a(i+7) = (random >> 24 & 0xFF).toByte
        }
        a
    }

    def find(data: Array[Array[Int]]) = {
        var counter = 0
        var innerCounter = 0
        var mainCounter = 0

        val start = new Date().getTime()
        while (counter < data.length) {
//            innerCounter = data(counter)(1)
            while (innerCounter > 0) {
//                innerCounter -= 1
                mainCounter += 1
            }
            counter += 1
        }

        println("done: " + (new Date().getTime() - start).toString + "ms")
        println("Num of encoded record: " + mainCounter)
    }

    def find(data: Array[Byte]) = {
        var counter = 0
        var innerCounter = 0
        var mainCounter = 0

        val start = new Date().getTime()
        while (counter < data.length/8) {
//            innerCounter = data(counter)(1)
            while (innerCounter > 0) {
                innerCounter -= 8
                mainCounter += 1
            }
            counter += 1
        }


        println("done: " + (new Date().getTime() - start).toString + "ms")
        println("Num of encoded record: " + mainCounter)
    }

    val data = genData
    find(data)
}
