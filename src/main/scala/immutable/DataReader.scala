package main.scala.immutable

import java.nio.MappedByteBuffer

/**
 * Created by marcin on 11/24/15.
 *
 * - takes the buffer or has access to it other ways
 * - understand data type
 * - exposes simple interface to return next value in buffer or seq of buffers
 */
// TODO: object vs class - when to know when to use which
class DataReader(mmaps: Seq[MappedByteBuffer]) {
    def DataReader(mmap: MappedByteBuffer) = {
        val mmaps = Seq(mmap)
    }

    def get() = {

    }
}
