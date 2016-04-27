package immutable.operators

import java.nio.IntBuffer

import immutable._
import immutable.encoders.{CharDescriptorBloom, EncoderDescriptor, Dict}
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/26/16.
  */

case class FetchSelectMatch(col: Column, items: Seq[String], op: SelectionOperator) extends SelectionOperator {
    debug(s"Using operator: $toString")

    SelectionOperator.prepareBuffer(
        col,
        SchemaManager.getTable(col.tblName)
    )

    override def toString = s"FetchSelect ${col.name} ${items}"

    def iterator = new FetchSelectIterator()

    val exactVal = col.enc match {
        case Dict => {
            val lookup = Dict.lookup(col)
            items.map(x => lookup.get(col.stringToValue(x)).get)
        }
        case _ => items.map(x => col.stringToValue(x))
    }

    class FetchSelectIterator extends Iterator[Int] {
        val encIter = col.getIterator
        val opIter = op.iterator

        def next = {
            var oid: Int = 0
            var success = false

            while (!success && opIter.hasNext) {
                val opOid = opIter.next
                encIter.seek(opOid)
                val tuple = encIter.next.asInstanceOf[(Int, col.DataType)]

                if (exactVal.contains(tuple._2)) {
                    oid = tuple._1
                    success = true
                }
            }
            oid
        }

        def hasNext = if (opIter.hasNext) true else false
    }

}

case class FetchSelectRange(col: Column, left: String, right: String, op: SelectionOperator) extends SelectionOperator {
    info(s"Using operator: $toString")

    SelectionOperator.prepareBuffer(
        col,
        SchemaManager.getTable(col.tblName)
    )

    override def toString = s"FetchSelect ${col.name} ${left}/${right}"

    def iterator = new FetchSelectRangeIterator()

    class FetchSelectRangeIterator extends Iterator[Int] {
        val minVal = col.stringToValue(left)
        val maxVal = col.stringToValue(right)

        val encIter = col.getIterator
        val opIter = op.iterator

        def next = {
            var oid: Int = 0
            var success = false

            while (!success && opIter.hasNext) {
                val opOid = opIter.next
                encIter.seek(opOid)
                val tuple = encIter.next.asInstanceOf[(Int, col.DataType)]

                if (col.ord.gteq(tuple._2, minVal) && col.ord.lteq(tuple._2, maxVal)) {
                    oid = tuple._1
                    success = true
                }
            }
            oid
        }

        def hasNext = if (opIter.hasNext) true else false
    }
}

