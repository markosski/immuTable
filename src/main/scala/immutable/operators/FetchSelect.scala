package immutable.operators

import java.nio.{IntBuffer, ByteBuffer}

import immutable._
import immutable.encoders.{Dict}
import immutable.LoggerHelper._

import scala.collection.mutable.BitSet

/**
  * Created by marcin on 2/26/16.
  */

case class FetchSelectMatch(col: Column, items: Seq[String], op: SelectionOperator) extends SelectionOperator {
    debug(s"Using operator: $toString")
    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)

    override def toString = s"FetchSelect ${col.name} ${items}"

    def iterator = new FetchSelectIterator()

    val exactVal = col.enc match {
        case Dict => {
            val lookup = Dict.lookup(col)
            items.map(x => lookup.get(col.stringToValue(x)).get)
        }
        case _ => items.map(x => col.stringToValue(x))
    }

    def getTuple(dataVec: (IntBuffer, ByteBuffer), valSize: Int) = {
        val bytes = new Array[Byte](valSize)
        dataVec._2.get(bytes)
        (dataVec._1.get, bytes)
    }

    class FetchSelectIterator extends Iterator[DataVector] {
        val encIter = col.getIterator
        val opIter = op.iterator

        def next = {
            var dataVec = opIter.next
            val dataVecSelected = dataVec.selected.iterator
            val selection = BitSet()
            // Create a colVec of size the same as first vector in DataVector
            val colVec = new Array[Any](dataVec.data(0).size)

            while (dataVecSelected.hasNext) {
                val localIdx = dataVecSelected.next
                encIter.seek(dataVec.vecID - colVec.size + localIdx)

                val value = col.enc match {
                    case Dict => encIter.next.asInstanceOf[Int]
                    case _ => encIter.next
                }

                if (exactVal.contains(value)) {
                    selection.add(localIdx)
                    colVec(localIdx) = value
                }
            }

            dataVec = dataVec.copy(
                cols=dataVec.cols :+ col,
                data=dataVec.data :+ colVec,
                selected=dataVec.selected & selection)

            dataVec
        }

        def hasNext = if (opIter.hasNext) true else false
    }
}

case class FetchSelectRange(col: Column, left: String, right: String, op: SelectionOperator) extends SelectionOperator {
    info(s"Using operator: $toString")
    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)

    override def toString = s"FetchSelect ${col.name} ${left}/${right}"

    def iterator = new FetchSelectRangeIterator()

    class FetchSelectRangeIterator extends Iterator[DataVector] {
        val minVal = col.stringToValue(left)
        val maxVal = col.stringToValue(right)

        val opIter = op.iterator
        val encIter = col.getIterator

        def next = {
            var dataVec = opIter.next  // get a data vector of Config.vectorSize
            val dataVecSelected = dataVec.selected.iterator
            val selection = BitSet()
            val colVec = new Array[Any](dataVec.data(0).size)

            while (dataVecSelected.hasNext) {
                val localIdx = dataVecSelected.next
                encIter.seek(dataVec.vecID - colVec.size + localIdx)
                val value = encIter.next.asInstanceOf[col.DataType]

                if (col.ord.gteq(value, minVal) && col.ord.lteq(value, maxVal)) {
                    selection.add(localIdx)
                    colVec(localIdx) = value
                }
            }

            dataVec = dataVec.copy(
                cols=dataVec.cols :+ col,
                data=dataVec.data :+ colVec,
                selected=dataVec.selected & selection)
            dataVec
        }

        def hasNext = if (opIter.hasNext) true else false
    }
}

