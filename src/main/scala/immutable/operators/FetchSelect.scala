package immutable.operators

import java.nio.ByteBuffer

import immutable.{Range, Exact, Table}
import immutable.encoders.{Dict, Encoder}
import immutable.{VarCharColumn, NumericColumn, Column}
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/26/16.
  */

object FetchSelect {
    def apply(pred: Exact, oidBuffer: ByteBuffer, useIntermediate: Boolean)
                (implicit table: Table): ByteBuffer = {
        debug("Enter FetchSelect")
        Operator.prepareBuffer(pred.col, table)

        val iter = pred.col.getIterator
        val result = ByteBuffer.allocateDirect(oidBuffer.limit)

        // Special case for Dict encoding where we need to string value has to be converted to Int.
        info("Start exactVal")
        val exactVal = pred.col.enc match {
            case Dict => {
                val lookup = Dict.lookup(pred.col)
                pred.value.map(x => lookup.get(pred.col.stringToValue(x)).get)
            }
            case _ => pred.value.map(x => pred.col.stringToValue(x))
        }

        debug(s"Start FetchSelect scan ${pred.col.name} ${pred.value}")

        var tuple = iter.next
        pred.col.enc match {
            case Dict => {
                while (oidBuffer.position < oidBuffer.limit) {
                    var oid = oidBuffer.getInt

                    while (tuple._1 > oid && oidBuffer.position < oidBuffer.limit) oid = oidBuffer.getInt

                    while (iter.hasNext && tuple._1 < oid) tuple = iter.next

                    if (tuple._1 == oid && exactVal.contains(tuple._2)) {
                        result.putInt(tuple._1)
                    }
                }
            }
            case _ => {
                while (oidBuffer.position < oidBuffer.limit) {
                    var oid = oidBuffer.getInt

                    while (tuple._1 > oid && oidBuffer.position < oidBuffer.limit) oid = oidBuffer.getInt

                    while (iter.hasNext && tuple._1 < oid) tuple = iter.next

                    if (tuple._1 == oid && exactVal.contains(tuple._2)) {
                        result.putInt(tuple._1)
                    }
                }
            }
        }
        info("End FetchSelect scan")
        oidBuffer.rewind
        result.flip
        result
    }


    def apply(pred: Range, oidBuffer: ByteBuffer, useIntermediate: Boolean)(implicit table: Table): ByteBuffer = {
        debug("Enter FetchSelect")
        Operator.prepareBuffer(pred.col, table)

        val iter = pred.col.getIterator
        val result = ByteBuffer.allocateDirect(oidBuffer.limit)
        val minVal = pred.col.stringToValue(pred.min)
        val maxVal = pred.col.stringToValue(pred.max)

        var tuple = iter.next

        info(s"Start FetchSelect ${pred.col.name} ${pred.min}/${pred.max}")
        while (oidBuffer.position < oidBuffer.limit) {
            var oid = oidBuffer.getInt

            while (tuple._1 > oid && oidBuffer.position < oidBuffer.limit) oid = oidBuffer.getInt

            while (tuple._1 < oid && iter.hasNext) tuple = iter.next

            if (pred.col.ord.gteq(tuple._2.asInstanceOf[pred.col.DataType], minVal) && pred.col.ord.lteq(tuple._2.asInstanceOf[pred.col.DataType], maxVal)) result.putInt(tuple._1)
        }
        info("End FetchSelect scan")
        oidBuffer.rewind
        result.flip
        result
    }
}

