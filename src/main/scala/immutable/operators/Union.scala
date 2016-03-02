package immutable.operators

import java.nio.ByteBuffer

import immutable.LoggerHelper._

import scala.math._

/**
  * Created by marcin on 2/27/16.
  */
object Union {
    def apply(bufferA: ByteBuffer, bufferB: ByteBuffer): ByteBuffer = {
        val result = ByteBuffer.allocateDirect(bufferA.limit + bufferB.limit)

        var oidB = bufferB.getInt

        while (bufferA.position < bufferA.limit) {
            var oidA = bufferA.getInt

            while (oidB > oidA && bufferA.position < bufferA.limit) {
                result.putInt(oidA)
                oidA = bufferA.getInt
            }

            while (oidB < oidA && bufferB.position < bufferB.limit) {
                result.putInt(oidB)
                oidB = bufferB.getInt
            }
        }

        bufferA.rewind
        bufferB.rewind
        info(s"Union A: ${bufferA.limit/4} B: ${bufferB.limit/4}")
        result.flip
        result
    }
}
