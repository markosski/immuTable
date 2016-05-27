package immutable.operators

import immutable.encoders.Dict
import immutable.{DataVector, SchemaManager, Column}
import immutable.LoggerHelper._

case class Fetch(col: Column, op: SelectionOperator) extends SelectionOperator {
    debug(s"Using operator: $toString")

    val table = SchemaManager.getTable(col.tblName)
    SelectionOperator.prepareBuffer(col, table)

    override def toString = s"Fetch ${col.name}"

    def iterator = new FetchIterator()

    class FetchIterator extends Iterator[DataVector] {
        val encIter = col.getIterator
        val opIter = op.iterator

        def next = {
            var dataVec = opIter.next
            val dataVecSelected = dataVec.selected.iterator
            // Create a colVec of size the same as first vector in DataVector
            val colVec = new Array[Any](dataVec.data(0).length)

            while (dataVecSelected.hasNext) {
                val localIdx = dataVecSelected.next
                encIter.seek(dataVec.vecID - dataVec.data(0).size + localIdx)

                val value = col.enc match {
                    case Dict => encIter.next.asInstanceOf[Int]
                    case _ => encIter.next
                }
                colVec(localIdx) = value
            }

            dataVec = dataVec.copy(
                cols=dataVec.cols :+ col,
                data=dataVec.data :+ colVec
            )

            dataVec
        }

        def hasNext = if (opIter.hasNext) true else false
    }
}