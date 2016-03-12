package immutable.operators

import java.nio.ByteBuffer

import immutable.{Range, Exact, Table}
import immutable.encoders.{Dict, Encoder, Intermediate}
import immutable.{VarCharColumn, NumericColumn, Column}
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/26/16.
  */

object FetchSelect {
    def apply[A](pred: Exact[A], oidBuffer: ByteBuffer, useIntermediate: Boolean)
                (implicit table: Table): ByteBuffer = {
        debug("Enter FetchSelect")
        Operator.prepareBuffer(pred.col, table)

        var iter = Encoder.getColumnIterator(pred.col, table)
        if (useIntermediate) {
            debug("Using intermediate.")
            iter = Intermediate(pred.col, table).iterator
        }
        val result = ByteBuffer.allocateDirect(oidBuffer.limit)

        // Special case for Dict encoding where we need to string value has to be converted to Int.
        val exactVal = {
            if (pred.col.encoder == 'Dict) {
                val lookup = Dict(pred.col, table).lookup
                pred.value.map(x => lookup.get(pred.col.stringToValue(x)).get)
            } else {
                pred.value.map(x => pred.col.stringToValue(x))
            }
        }

        debug(s"Start FetchSelect scan ${pred.col.name} ${pred.value}")

        var tuple = iter.next
        if (pred.col.encoder == 'Dict) {
            while (oidBuffer.position < oidBuffer.limit) {
                var oid = oidBuffer.getInt

                while (tuple._1 > oid && oidBuffer.position < oidBuffer.limit) oid = oidBuffer.getInt

                while (iter.hasNext && tuple._1 < oid) tuple = iter.next

                if (tuple._1 == oid && exactVal.contains(tuple._2)) {
                    result.putInt(tuple._1)
                }
            }
        } else {
            while (oidBuffer.position < oidBuffer.limit) {
                var oid = oidBuffer.getInt

                while (tuple._1 > oid && oidBuffer.position < oidBuffer.limit) oid = oidBuffer.getInt

                while (iter.hasNext && tuple._1 < oid) tuple = iter.next

                if (tuple._1 == oid && exactVal.contains(tuple._2.asInstanceOf[A])) {
                    result.putInt(tuple._1)
                }
            }
        }
        info("End FetchSelect scan")
        oidBuffer.rewind
        result.flip
        result
    }


    def apply[A](pred: Range[A], oidBuffer: ByteBuffer, useIntermediate: Boolean)
                (implicit table: Table, numeric: Numeric[A]): ByteBuffer = {
        debug("Enter FetchSelect")
        Operator.prepareBuffer(pred.col, table)

        var iter = Encoder.getColumnIterator(pred.col, table)
        if (useIntermediate) {
            debug("Using intermediate.")
            iter = Intermediate(pred.col, table).iterator
        }
        val result = ByteBuffer.allocateDirect(oidBuffer.limit)
        val minVal = pred.col.stringToValue(pred.min)
        val maxVal = pred.col.stringToValue(pred.max)

        var tuple = iter.next

        info(s"Start FetchSelect ${pred.col.name} ${pred.min}/${pred.max}")
        while (oidBuffer.position < oidBuffer.limit) {
            var oid = oidBuffer.getInt

            while (tuple._1 > oid && oidBuffer.position < oidBuffer.limit) oid = oidBuffer.getInt

            while (tuple._1 < oid && iter.hasNext) tuple = iter.next

            if (numeric.gteq(tuple._2.asInstanceOf[A], minVal) && numeric.lteq(tuple._2.asInstanceOf[A], maxVal)) result.putInt(tuple._1)
        }
        info("End FetchSelect scan")
        oidBuffer.rewind
        result.flip
        result
    }
}

