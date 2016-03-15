package immutable.operators

import java.nio.{IntBuffer, ByteBuffer}

import immutable._
import immutable.encoders.{Dict}
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/26/16.
  */

case class FetchSelectMatch(col: Column, items: Seq[String], op: SelectionOperator) extends SelectionOperator {
    debug(s"Start FetchSelect scan ${col.name} ${items}")
    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)
    val iterator = new FetchSelectIterator()

    class FetchSelectIterator extends Iterator[IntBuffer] {
        val result = IntBuffer.allocate(Config.vectorSize)
        val encIter = col.getIterator
        var oids = IntBuffer.allocate(Config.vectorSize)
        oids.flip

        val exactVal = col.enc match {
            case Dict => {
                val lookup = Dict.lookup(col)
                items.map(x => lookup.get(col.stringToValue(x)).get)
            }
            case _ => items.map(x => col.stringToValue(x))
        }

        def next = {
            result.clear

            col.enc match {
                case Dict => {
                    while (encIter.hasNext && result.hasRemaining) {
                        if (!oids.hasRemaining && op.iterator.hasNext) oids = op.iterator.next

                        if (oids.hasRemaining) {
                            var tuple = encIter.next
                            var oid = oids.get

                            while (tuple._1 > oid && oids.hasRemaining) oid = oids.get

                            while (tuple._1 < oid && encIter.hasNext) tuple = encIter.next

                            if (tuple._1 == oid && exactVal.contains(tuple._2)) {
                                result.put(tuple._1)
                            }
                        } else {
                            result.limit(result.position) // will cause hasRamaining == false
                        }
                    }
                }
                case _ => {
//                    while (encIter.hasNext && result.hasRemaining) {
//                        if (!oids.hasRemaining && op.iterator.hasNext) oids = op.iterator.next
//
//                        if (oids.hasRemaining) {
//                            var tuple = encIter.next
//                            var oid = oids.get
//
//                            while (tuple._1 > oid && oids.hasRemaining) oid = oids.get
//
//                            while (tuple._1 < oid && encIter.hasNext) tuple = encIter.next
//
//                            if (tuple._1 == oid && exactVal.contains(tuple._2)) {
//                                result.put(tuple._1)
//                            }
//                        } else {
//                            result.limit(result.position)
//                        }
//                    }
                }
            }
            result.flip
            result
        }

        def hasNext = if (op.iterator.hasNext) true else false
    }
}

case class FetchSelectRange(col: Column, min: String, max: String, op: SelectionOperator) extends SelectionOperator {
    info(s"Start FetchSelect ${col.name} ${min}/${max}")
    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)
    val iterator = new FetchSelectRangeIterator()
    val minVal = col.stringToValue(min)
    val maxVal = col.stringToValue(max)

    class FetchSelectRangeIterator extends Iterator[IntBuffer] {
        val encIter = col.getIterator
        val result = IntBuffer.allocate(Config.vectorSize)
        var oids = IntBuffer.allocate(Config.vectorSize)
        oids.flip

        def next = {
            result.clear

            while (encIter.hasNext && result.hasRemaining) {
                if (!oids.hasRemaining && op.iterator.hasNext) oids = op.iterator.next

                if (oids.hasRemaining) {
                    var tuple = encIter.next
                    var oid = oids.get()

                    while (tuple._1 > oid && oids.hasRemaining) oid = oids.get()

                    while (tuple._1 < oid && encIter.hasNext) tuple = encIter.next

                    if (col.ord.gteq(tuple._2.asInstanceOf[col.DataType], minVal)
                            && col.ord.lteq(tuple._2.asInstanceOf[col.DataType], maxVal)) {
                        result.put(tuple._1)
                    }
                } else {
                    result.limit(result.position)
                }
            }

            result.flip
            result
        }

        def hasNext = if (op.iterator.hasNext) true else false
    }


}

