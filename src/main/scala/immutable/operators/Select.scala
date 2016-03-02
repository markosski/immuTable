package immutable.operators

import java.nio.ByteBuffer

import immutable.{Contains, Range, Exact, Table}
import immutable.encoders.{Encoder, Intermediate}
import immutable.{VarCharColumn, NumericColumn, Column}
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/26/16.
  */

object Select {
    def apply[A](pred: Range[A], useIntermediate: Boolean)(implicit table: Table, numeric: Numeric[A]): ByteBuffer = {
        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        // TODO: Make it so we're not loading iterator twice
        var iter = Encoder.getColumnIterator(pred.col, table)
        if (useIntermediate) {
            debug("Using intermediate.")
            iter = Intermediate(pred.col, table).iterator
        }
        val result = ByteBuffer.allocate(table.size * 4)
        val minVal = pred.col.stringToValue(pred.min)
        val maxVal = pred.col.stringToValue(pred.max)

        info(s"Start Select scan ${pred.col.name} ${pred.min}/${pred.max}")
        while(iter.hasNext) {
            val tuple = iter.next
            if (numeric.gteq(tuple._2, minVal) && numeric.lteq(tuple._2, maxVal)) {
                result.putInt(tuple._1)
            }
        }
        info("End Select scan")
        result.flip
        result
    }

    def apply[A](pred: Exact[A], useIntermediate: Boolean)(implicit table: Table): ByteBuffer = {
        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        var iter = Encoder.getColumnIterator(pred.col, table)
        if (useIntermediate) {
            debug("Using intermediate.")
            iter = Intermediate(pred.col, table).iterator
        }

        val result = ByteBuffer.allocate(table.size * 4)

        val exactVal = pred.col match {
            case col: VarCharColumn => pred.value.map(x => x.toInt)
            case _ => pred.value.map(x => pred.col.stringToValue(x))
        }

        info(s"Start Select scan ${pred.col.name} ${pred.value}")
        while(iter.hasNext) {
            val tuple = iter.next
            if (exactVal == tuple._2) result.putInt(tuple._1)
        }
        info(s"End Select scan")
        result.flip
        result
    }

    def apply[A](pred: Contains[A], useIntermediate: Boolean)(implicit table: Table): ByteBuffer = {
        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        var iter = Encoder.getColumnIterator(pred.col, table)
        if (useIntermediate) {
            debug("Using intermediate.")
            iter = Intermediate(pred.col, table).iterator
        }

        val result = ByteBuffer.allocate(table.size * 4)

        val exactVal = pred.col.stringToValue(pred.value)

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

