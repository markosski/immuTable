package immutable.operators

import java.nio.ByteBuffer

import immutable._
import immutable.encoders.{Dict, Encoder, Intermediate}
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/26/16.
  */

object Select {
    def apply[A](pred: Range[A], useIntermediate: Boolean)(implicit table: Table, num: Numeric[A]): ByteBuffer = {

        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        // TODO: Make it so we're not loading iterator twice
        var iter = Encoder.getColumnIterator(pred.col, table)
        if (useIntermediate) {
            debug("Using intermediate.")
            iter = Intermediate(pred.col, table).iterator
        }
        val result = ByteBuffer.allocateDirect(table.size * 4)
        val minVal = pred.col.stringToValue(pred.min)
        val maxVal = pred.col.stringToValue(pred.max)

        info(s"Start Select scan ${pred.col.name} ${pred.min}/${pred.max}")
        while(iter.hasNext) {
            val tuple = iter.next
            if (num.gteq(tuple._2.asInstanceOf[A], minVal) && num.lteq(tuple._2.asInstanceOf[A], maxVal)) {
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

        val result = ByteBuffer.allocateDirect(table.size * 4)

        // Special case for Dict encoding where we need to string value has to be converted to Int.
        val exactVal = {
            if (pred.col.encoder == 'Dict) {
                val lookup = Dict(pred.col, table).lookup
                pred.value.map(x => lookup.get(pred.col.stringToValue(x)).get)
            } else {
                pred.value.map(x => pred.col.stringToValue(x))
            }
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

    def apply[A](pred: Contains[A], useIntermediate: Boolean)(implicit table: Table): ByteBuffer = {
        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        var iter = Encoder.getColumnIterator(pred.col, table)
        if (useIntermediate) {
            debug("Using intermediate.")
            iter = Intermediate(pred.col, table).iterator
        }

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

    def apply[A](pred: Filter[A], useIntermediate: Boolean)(implicit table: Table): ByteBuffer = {
        info("Enter Select")
        Operator.prepareBuffer(pred.col, table)

        var iter = Encoder.getColumnIterator(pred.col, table)
        if (useIntermediate) {
            debug("Using intermediate.")
            iter = Intermediate(pred.col, table).iterator
        }

        val result = ByteBuffer.allocate(table.size * 4)

        info(s"Start Select scan ${pred.col.name} ${pred.func}")
        while(iter.hasNext) {
            val tuple = iter.next
            if (pred.func(tuple._2.asInstanceOf[A])) result.putInt(tuple._1)
        }
        info(s"End Select scan")
        result.flip
        result
    }
}

