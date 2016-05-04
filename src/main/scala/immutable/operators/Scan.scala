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

case class Scan(col: Column) extends SelectionOperator {
    debug(s"Using operator: $toString")

    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)

    def iterator = new SelectIterator()

    override def toString = s"Scan ${col.name}"

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
                selection.add(counter)
                colVec(counter) = value
                counter += 1
            }

            vecCounter += vecSize
            DataVector(vecCounter, List(col), List(colVec), selection)
        }

        def hasNext = if (encIter.hasNext) true else false
    }
}

