package immutable.operators

import java.nio.{IntBuffer, ByteBuffer}

import immutable._
import immutable.encoders.{Dict}
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

    class FetchSelectIterator extends Iterator[IntBuffer] {
        val encIter = col.getIterator
        val opIter = op.iterator
        var oids = IntBuffer.allocate(Config.vectorSize)
        if (opIter.hasNext)
            oids = opIter.next

        def next = {
            val result = IntBuffer.allocate(Config.vectorSize)

            while (result.hasRemaining && encIter.hasNext) {
                if (!oids.hasRemaining && opIter.hasNext)
                    oids = opIter.next

                if (oids.hasRemaining) {
                    var tuple = encIter.next
                    var oid = oids.get

                    while (tuple._1 > oid && oids.hasRemaining)
                        oid = oids.get

                    while (tuple._1 < oid && encIter.hasNext)
                        tuple = encIter.next

                    if (tuple._1 == oid && exactVal.contains(tuple._2))
                        result.put(tuple._1)

                } else {
                    result.limit(result.position)
                }
            }

//            col.enc match {
//                case Dict => {
//                }
//                case _ => {  }
//            }
            result.flip
            result
        }

        def hasNext = if (oids.hasRemaining || opIter.hasNext) true else false
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

    class FetchSelectRangeIterator extends Iterator[IntBuffer] {
        val minVal = col.stringToValue(left)
        val maxVal = col.stringToValue(right)

        val encIter = col.getIterator
        val opIter = op.iterator
        var oids = IntBuffer.allocate(Config.vectorSize)
        if (opIter.hasNext)
            oids = opIter.next

        def next = {
            val result = IntBuffer.allocate(Config.vectorSize)

            while (result.hasRemaining && encIter.hasNext) {
                if (!oids.hasRemaining && opIter.hasNext)
                    oids = opIter.next

                if (oids.hasRemaining) {
                    encIter.seek(oids.get)
                    val tuple = encIter.next.asInstanceOf[(Int, col.DataType)]

                    if (col.ord.gteq(tuple._2, minVal) && col.ord.lteq(tuple._2, maxVal)) {
                        result.put(tuple._1)
                    }
                } else {
                    result.limit(result.position)
                }
            }


//            while (result.hasRemaining && encIter.hasNext) {
//                if (!oids.hasRemaining && opIter.hasNext)
//                    oids = opIter.next
//
//                if (oids.hasRemaining) {
//                    var tuple = encIter.next.asInstanceOf[(Int, col.DataType)]
//                    var oid = oids.get
//
//                    while (tuple._1 > oid && oids.hasRemaining)
//                        oid = oids.get
//
//                    while (tuple._1 < oid && encIter.hasNext)
//                        tuple = encIter.next.asInstanceOf[(Int, col.DataType)]
//
//                    if (col.ord.gteq(tuple._2, minVal) && col.ord.lteq(tuple._2, maxVal)) {
//                        result.put(tuple._1)
//                    }
//                } else {
//                    result.limit(result.position)
//                }
//            }
            result.flip
            result
        }

        def hasNext = if (oids.hasRemaining || opIter.hasNext) true else false
    }
}

