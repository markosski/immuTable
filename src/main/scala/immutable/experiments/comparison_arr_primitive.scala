package immutable.experiments

import java.nio.{ByteBuffer, IntBuffer}
import immutable.LoggerHelper._
import immutable.helpers.Conversions

import scala.util.Random

/**
  * Created by marcin on 5/17/16.
  */
object comparison_arr_primitive extends App {

    val size = 100000000
    def heap_buff = {
        var counter = 0

        val buff = IntBuffer.allocate(size)

        while (buff.position < buff.capacity) {
            buff.put(Random.nextInt)
        }
        buff.flip()
        buff
    }

    def offheap_buff = {
        var counter = 0

        val buff = ByteBuffer.allocateDirect(size * 4)
        while (buff.position < buff.capacity) {
            buff.put(Conversions.intToBytes(Random.nextInt))
        }

        buff.flip()
        buff
    }

//    val buff = heap_buff
//    info("Start")
//    var counter = 0
//    val compare = 50000
//    var result = 0
//    while (counter < 100000000) {
//        if (buff.get > compare) {
//            result += 1
//        }
//        counter += 1
//    }

    val buff = offheap_buff
    info("Start")
    var counter = 0
    val compare = 50000
    var result = 0
    while (counter < 100000000) {
        if (buff.getInt > compare) {
            result += 1
        }
        counter += 1
    }
    info("Finish")
    println(s"Result: ${result}")
}
