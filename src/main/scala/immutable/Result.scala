package immutable

import java.nio.{ByteBuffer, MappedByteBuffer}

import com.sun.xml.internal.ws.util.ByteArrayBuffer
import immutable.helpers.Conversions

import scala.collection.mutable.ArrayBuffer

/**
 * Created by marcin on 9/9/15.
 *
 * Try parallel process each binary operation.
 *
 * a = Select(first_name, "Marcin")
 * b = SelectRange(age, 25, 35)
 * res = Intersect(a, b)
 *
 * a = Select(first_name, "Marcin")
 * b = Select(last_name, "Kossakowski")
 * c = Intersect(a, b)
 * res = fetch(first_name, last_name)(c)
 *
 */
//case class Result(buffer: ArrayBuffer[Int]) {
//    override val size = buffer.length

//    class ResultIterator(buffer: ArrayBuffer[Int]) extends Iterator[Int] {
//        val bufferIter = buffer.sliding(4, 4)
//        def next = {
//            Conversions.bytesToInt(bufferIter.next.toArray)
//        }
//
//        def hasNext = {
//            if (bufferIter.hasNext) true
//            else false
//        }
//    }

//    val iterator = new ResultIterator(buffer)
//}
