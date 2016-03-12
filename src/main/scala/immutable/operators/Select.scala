package immutable.operators

import java.nio.ByteBuffer

import immutable._
import immutable.encoders.{Dict, Encoder}
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/26/16.
  */

object Select {
    def apply[A](pred: Range, useIntermediate: Boolean)(implicit table: Table, num: Numeric[A]): ByteBuffer = {
        type A = pred.col.A
        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        // TODO: Make it so we're not loading iterator twice
        val iter = pred.col.getIterator
        val result = ByteBuffer.allocateDirect(table.size * 4)
        val minVal = pred.col.stringToValue(pred.min)
        val maxVal = pred.col.stringToValue(pred.max)

        info(s"Start Select scan ${pred.col.name} ${pred.min}/${pred.max}")
        while(iter.hasNext) {
            val tuple = iter.next
            if (pred.col.ord.gteq(tuple._2.asInstanceOf[A], minVal) && pred.col.ord.lteq(tuple._2.asInstanceOf[A], maxVal)) {
                result.putInt(tuple._1)
            }
        }
        info("End Select scan")
        result.flip
        result
    }

    def apply(pred: Exact, useIntermediate: Boolean)(implicit table: Table): ByteBuffer = {
        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        val iter = pred.col.getIterator
        val result = ByteBuffer.allocateDirect(table.size * 4)

        // Special case for Dict encoding where we need to string value has to be converted to Int.
        val exactVal = pred.col.enc match {
            case Dict => {
                val lookup = Dict.lookup(pred.col)
                pred.value.map(x => lookup.get(pred.col.stringToValue(x)).get)
            }
            case _ => pred.value.map(x => pred.col.stringToValue(x))
        }

        info(s"Start Select scan ${pred.col.name} ${pred.value}")

        // Branching out depending if we're testing one value or more.
        // Would like to find out if compile figure it out and .contains() should be used instead.
        if (pred.value.size == 1) {
            while(iter.hasNext) {
                val tuple = iter.next
                if (exactVal == tuple._2) result.putInt(tuple._1)
            }
        } else {
            while(iter.hasNext) {
                val tuple = iter.next
                if (exactVal.contains(tuple._2)) result.putInt(tuple._1)
            }
        }
        info(s"End Select scan")
        result.flip
        result
    }

    def apply(pred: Contains, useIntermediate: Boolean)(implicit table: Table): ByteBuffer = {
        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        var iter = pred.col.getIterator
        val result = ByteBuffer.allocate(table.size * 4)

        info(s"Start Select scan ${pred.col.name} ${pred.value}")
        while(iter.hasNext) {
            val tuple = iter.next
            if (pred.value.contains(tuple._2)) result.putInt(tuple._1)
        }
        info(s"End Select scan")
        result.flip
        result
    }
}

