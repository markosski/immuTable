package immutable.operators

import java.nio.{IntBuffer, ByteBuffer}

import immutable._
import immutable.encoders.{Dict, Encoder}
import immutable.LoggerHelper._
import immutable.DataVector
import immutable.helpers.Conversions
import scala.math

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

case class ScanSelectRange(col: Column, left: String, right: String) extends SelectionOperator {
    debug(s"Using operator: $toString")

    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)

    def iterator = new SelectIterator()
    val minVal = col.stringToValue(left)
    val maxVal = col.stringToValue(right)

    override def toString = s"Select ${left}/${right}"

    /**
      * On each call return a vector of size Config.vectorSize.
      * Caller is responsible to exhaust the vector and call next() again on iterator.
      */
    class SelectIterator extends Iterator[DataVector] {
        val encIter = col.getIterator
        var vecCounter = 0

        def next = {
            val selection = BitSet()
            val vecSize = math.min(Config.vectorSize, table.size - encIter.position)
            val colVec = new Array[Any](vecSize)

            var counter = 0
            while (encIter.hasNext && counter < vecSize) {
                val value = encIter.next.asInstanceOf[col.DataType]

                if (col.ord.gteq(value, minVal) && col.ord.lteq(value, maxVal)) {
                    selection.add(counter)
                    colVec(counter) = value
                }
                counter += 1
            }

            vecCounter += vecSize
            DataVector(vecCounter, List(col), List(colVec), selection)
        }

        def hasNext = if (encIter.hasNext) true else false
    }
}

case class ScanSelectMatch(col: Column, items: Seq[String]) extends SelectionOperator {
    debug(s"Using operator: $toString")
    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)

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
        val encIter = col.getIterator
        var vecCounter = 0

        def next = {
            val selection = BitSet()
            val vecSize = math.min(Config.vectorSize, table.size - encIter.position)
            val colVec = new Array[Any](vecSize)
            var counter = 0

            // Branching out depending if we're testing one value or more.
            if (exactVal.size == 1) {
                while (encIter.hasNext && counter < vecSize) {
                    val value = encIter.next.asInstanceOf[col.DataType]

                    if (exactVal(0) == value) {
                        selection.add(counter)
                        colVec(counter) = value
                    }
                    counter += 1
                }

                val vec = DataVector(
                    vecCounter + vecSize,
                    List(col),
                    List(colVec),
                    selection)

                vecCounter += 1
                vec

            } else {
                while (encIter.hasNext && counter < vecSize) {
                    val value = encIter.next.asInstanceOf[col.DataType]

                    if (exactVal.contains(value)) {
                        selection.add(counter)
                        colVec(counter) = value
                    }
                    counter += 1
                }

                val vec = DataVector(
                    vecCounter + vecSize,
                    List(col),
                    List(colVec),
                    selection)

                vecCounter += 1
                vec
            }
        }

        def hasNext = if (encIter.hasNext) true else false
    }
}

