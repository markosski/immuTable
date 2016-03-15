package immutable.operators

import java.nio.{IntBuffer, ByteBuffer}

import immutable._
import immutable.encoders.{Dict, Encoder}
import immutable.LoggerHelper._

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

case class SelectRange(col: Column, min: String, max: String) extends SelectionOperator {
    info(s"Invoke SelectRange scan ${col.name} ${min}/${max}")
    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)

    val iterator = new SelectIterator()
    val minVal = col.stringToValue(min)
    val maxVal = col.stringToValue(max)

    /**
      * On each call return a vector of size Config.vectorSize.
      * Caller is responsible to exhaust the vector and call next() again on iterator.
      */
    class SelectIterator extends Iterator[IntBuffer] {
        val result = IntBuffer.allocate(Config.vectorSize)
        val encIter = col.getIterator

        def next = {
            result.clear

            while(encIter.hasNext && result.hasRemaining) {
                val tuple = encIter.next
                if (col.ord.gteq(tuple._2.asInstanceOf[col.DataType], minVal) && col.ord.lteq(tuple._2.asInstanceOf[col.DataType], maxVal)) {
                    result.put(tuple._1)
                }
            }

            result.flip
            result
        }

        def hasNext = if (encIter.hasNext) true else false
    }
}

case class Select(col: Column, items: Seq[String]) extends SelectionOperator {
    info(s"Start Select scan ${col.name} ${items}")

    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)

    val iterator = new SelectIterator()

    class SelectIterator extends Iterator[IntBuffer] {
        val result = IntBuffer.allocate(Config.vectorSize)
        val encIter = col.getIterator

        // Special case for Dict encoding where we need to string value has to be converted to Int.
        val exactVal = col.enc match {
            case Dict => {
                val lookup = Dict.lookup(col)
                items.map(x => lookup.get(col.stringToValue(x)).get)
            }
            case _ => items.map(x => col.stringToValue(x))
        }

        def next = {
            result.clear

            // Branching out depending if we're testing one value or more.
            // Would like to find out if compile figure it out and .contains() should be used instead.
            if (exactVal.size == 1) {
                while(encIter.hasNext && result.hasRemaining) {
                    val tuple = encIter.next
                    if (exactVal(0) == tuple._2) result.put(tuple._1)
                }
            } else {
                while(encIter.hasNext && result.hasRemaining) {
                    val tuple = encIter.next
                    if (exactVal.contains(tuple._2)) {
                        result.put(tuple._1)
                    }
                }
            }

            result.flip
            result
        }

        def hasNext = if (encIter.hasNext) true else false
    }
}

