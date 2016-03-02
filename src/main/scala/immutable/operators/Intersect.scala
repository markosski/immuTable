package immutable.operators

import java.nio.ByteBuffer
import math.max
import immutable.LoggerHelper._
import immutable.helpers.{Timer}

/**
  * Created by marcin on 2/26/16.
  */
object Intersect {
    def apply(bufferA: ByteBuffer, bufferB: ByteBuffer): ByteBuffer = {
        val result = ByteBuffer.allocateDirect(max(bufferA.limit, bufferB.limit))

        var oidB = bufferB.getInt

        while (bufferA.position < bufferA.limit) {
            var oidA = bufferA.getInt

            while (oidB > oidA && bufferA.position < bufferA.limit) oidA = bufferA.getInt

            while (oidB < oidA && bufferB.position < bufferB.limit) oidB = bufferB.getInt

            if (oidA == oidB) result.putInt(oidA)
        }

        bufferA.rewind
        bufferB.rewind
        info(s"Intersect A: ${bufferA.limit/4} B: ${bufferB.limit/4}")
        result.flip
        result
    }
}
