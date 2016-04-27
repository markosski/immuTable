package immutable.operators

import java.nio.{IntBuffer, ByteBuffer}

import immutable._
import immutable.encoders.{Dict, Encoder}
import immutable.LoggerHelper._
import immutable.DataVector
import immutable.helpers.Conversions

import scala.collection.mutable.BitSet

/**
  * Created by marcin on 2/26/16.
  */
/*
abstract Expr
case class Num(num: Double) extends Expr
case class Col(col: Column) extends Expr
case class BinOp(f: (a: Expr, b: Expr) => Expr, a: Expr, b: Expr) extends Expr

FetchSelect(
    FetchSelect(
        Select(
            BinOp(op: (a: Num, b: Num) => Num, Col("age"), Num(2)),
            Range(25, 50)
        ),
        Expr(Col("fname"), Exact(List("Marcin", "Melissa"))
    ),
    Expr(Col("score1") + Expr(Col("score2")), Range(75, 100)
)
*/

case class SelectRange(col: Column, left: String, right: String, data: DataVectorProducer) extends SelectionOperator {
    debug(s"Using operator: $toString")

    val table = SchemaManager.getTable(col.tblName)

    def iterator = new SelectIterator()
    val minVal = col.stringToValue(left)
    val maxVal = col.stringToValue(right)

    override def toString = s"Select ${left}/${right}"

    /**
      * On each call return a vector of size Config.vectorSize.
      * Caller is responsible to exhaust the vector and call next() again on iterator.
      */
    class SelectIterator extends Iterator[DataVector] {
        val dataIter = data.iterator
        var dataVecColPos: Int = 0

        def next = {
            val dataVec = dataIter.next
            val dataVecSelected = dataVec.selected.iterator
            val size = dataVec.data(0).size
            val selection = BitSet()

            dataVec.cols.zipWithIndex.foreach(x => if (x._1.name == col.name) dataVecColPos = x._2)

            while (dataVecSelected.hasNext) {
                val selected = dataVecSelected.next
                val value = col.enc match {
                    case Dict => dataVec.data(dataVecColPos)(selected)
                    case _ => dataVec.data(dataVecColPos)(selected)
                }

                if (col.ord.gteq(value.asInstanceOf[col.DataType], minVal) && col.ord.lteq(value.asInstanceOf[col.DataType], maxVal)) {
                    selection.add(selected)
                }
            }

            dataVec.selected = dataVec.selected & selection
            dataVec
        }

        def hasNext = if (dataIter.hasNext) true else false
    }
}

case class SelectMatch(col: Column, items: Seq[String], data: DataVectorProducer) extends SelectionOperator {
    debug(s"Using operator: $toString")

    def iterator = new SelectIterator()

    override def toString = s"Select ${items}"

    class SelectIterator extends Iterator[DataVector] {
        val exactVal = col.enc match {
            case Dict => {
                val lookup = Dict.lookup(col)
                items.map(x => lookup.get(col.stringToValue(x)).get)
            }
            case _ => items.map(x => col.stringToValue(x))
        }
        val dataIter = data.iterator
        var dataVecCounter = 0
        var dataVecColPos: Int = _

        def next = {
            val dataVec = dataIter.next
            val size = dataVec.data(0).size

            dataVec.cols.zipWithIndex.foreach(x => if (x._1 == col.name) dataVecColPos = x._2)
            val selection = BitSet()

            // Branching out depending if we're testing one value or more.
            if (exactVal.size == 1) {
//                while (dataVecCounter < size - 1) {
//                    val value = col.bytesToValue(dataVec.data(dataVecColPos).slice(dataVecCounter * col.size, dataVecCounter * col.size + col.size))
//
//                    if (exactVal.contains(value)) {
//                        selection.add(dataVecCounter)
//                    }
//                    dataVecCounter += 1
//                }

                dataVec.selected = dataVec.selected & selection

            } else {
//                while (dataVecCounter < size - 1) {
//                    val value = col.enc match {
//                        case Dict => Conversions.bytesToInt(dataVec.data(dataVecColPos).slice(dataVecCounter * 4, dataVecCounter * 4 + 4))
//                        case _ => col.bytesToValue(dataVec.data(dataVecColPos).slice(dataVecCounter * col.size, dataVecCounter * col.size + col.size))
//                    }

//                    if (exactVal.contains(value.asInstanceOf[col.DataType])) {
//                        selection.add(dataVecCounter)
//                    }
//                    dataVecCounter += 1
//                }

                dataVec.selected = dataVec.selected & selection
            }
            dataVecCounter = 0

            dataVec
        }

        def hasNext = if (dataIter.hasNext) true else false
    }
}

