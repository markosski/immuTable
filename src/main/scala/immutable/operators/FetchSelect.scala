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

    def getTuple(dataVec: (IntBuffer, ByteBuffer), valSize: Int) = {
        val bytes = new Array[Byte](valSize)
        dataVec._2.get(bytes)
        (dataVec._1.get, bytes)
    }

    class FetchSelectIterator extends Iterator[DataVector] {
        val opIter = op.iterator
        var dataVecCounter = 0
        var dataVecColPos: Int = _

        def next = {
            var dataVec = opIter.next
            while (dataVec.selected.size == 0 && hasNext)
                dataVec = opIter.next

            val size = dataVec.data(0).size

            dataVec.cols.zipWithIndex.foreach(x => if (x._1 == col.name) dataVecColPos = x._2)
            var selection = BitSet()

            while (dataVecCounter < size - 1) {
                val value = dataVec.data(dataVecColPos)(dataVecCounter)

                if (exactVal.contains(value)) {
                    selection.add(dataVecCounter)
                }
                dataVecCounter += 1
            }

            dataVec.selected = dataVec.selected & selection
            dataVec
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

    class FetchSelectRangeIterator extends Iterator[DataVector] {
        val minVal = col.stringToValue(left)
        val maxVal = col.stringToValue(right)

        val opIter = op.iterator
        var dataVecCounter = 0
        var dataVecColPos: Int = _

        def next = {
            var dataVec = opIter.next  // get a data vector of Config.vectorSize
            while (dataVec.selected.size == 0 && hasNext)
                dataVec = opIter.next

            val size = dataVec.data(0).size

            dataVec.cols.zipWithIndex.foreach(x => if (x._1 == col.name) dataVecColPos = x._2)
            var selection = BitSet()

            while (dataVecCounter < size - 1) {
                val value = dataVec.data(dataVecColPos)(dataVecCounter)

                if (col.ord.gteq(value.asInstanceOf[col.DataType], minVal) && col.ord.lteq(value.asInstanceOf[col.DataType], maxVal)) {
                    selection.add(dataVecCounter)
                }
                dataVecCounter += 1
            }

            dataVec.selected = dataVec.selected & selection

            dataVecCounter = 0
            dataVec
        }

        def hasNext = if (opIter.hasNext) true else false
    }
}

