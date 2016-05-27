package immutable.operators

import immutable.{Column, DataVector}

import scala.collection.mutable

/**
  * Created by marcin on 5/25/16.
  * - need to consume right operator B
  */
case class HashJoin(opLeft: SelectionOperator, opRight: SelectionOperator, joinColA: Column, joinColB: Column, keepCols: List[Column]) extends SelectionOperator {
    // TODO: instead of List[Int] use byte array
    val rightHash = mutable.HashMap[joinColB.DataType, List[Int]]()
    val rightIter = opRight.iterator
    val leftIter = opLeft.iterator

    def iterator = new JoinIterator()

    def resolveDataVecColIdx(dataVec: DataVector, cols: List[Column]) = {
        var colIdxLookup = Map[String, Int]()

        for (i <- 0 until cols.size) {
            dataVec.cols.map(x => x.name).zipWithIndex.find(x => x._1 == cols(i).name) match {
                case Some(x) => colIdxLookup = colIdxLookup + x
                case _ => throw new Exception("Could not match column in resolveDataVecColIdx")
            }
        }
        colIdxLookup
    }

    while (rightIter.hasNext) {
        val vec = rightIter.next
        val selectedIter = vec.selected.iterator
        val rightColIdx = resolveDataVecColIdx(vec, vec.cols)
        while (selectedIter.hasNext) {
            val localIdx = selectedIter.next
            val oid = vec.vecID - vec.data(0).size + localIdx
            rightHash.get(vec.data(rightColIdx.get(joinColB.name).get)(localIdx).asInstanceOf[joinColB.DataType]) match {
                case Some(x) => rightHash.update(vec.data(rightColIdx.get(joinColB.name).get)(localIdx).asInstanceOf[joinColB.DataType], oid :: x)
                case None => rightHash += (vec.data(rightColIdx.get(joinColB.name).get)(localIdx).asInstanceOf[joinColB.DataType] -> List(oid))
            }
        }
    }

    class JoinIterator extends Iterator[DataVector] {
        var dataVec = leftIter.next
        var selected = dataVec.selected.iterator
        val dataVecColIdx: Map[String, Int] = resolveDataVecColIdx(dataVec, dataVec.cols)
        val leftColJoinIdx = dataVecColIdx.get(joinColA.name).get
        val rightKeepColsIters = for (col <- keepCols) yield col.getIterator

        def next = {
            val joinSelection = mutable.BitSet()
            val colVecs = for (i <- keepCols.indices) yield new Array[Any](dataVec.data.head.length)

            while (selected.hasNext) {
                // Left vector local idx
                val localIdx = selected.next

                rightHash.get(dataVec.data(leftColJoinIdx)(localIdx).asInstanceOf[joinColB.DataType]) match {
                    case Some(x) => {
                        // For each column create colVec and attach to vector
                        // For any new columns that have to be created create new vector
                        joinSelection.add(localIdx)
                        for (i <- keepCols.indices) {
                            rightKeepColsIters(i).seek(0)  // TODO: for testing purposes pick 1st element
                            val value = rightKeepColsIters(i).next
                            colVecs(i)(localIdx) = value
                        }
                    }
                    case None => Unit
                }
            }

            dataVec.copy(
                cols=dataVec.cols ++ keepCols,
                data=dataVec.data ++ colVecs,
                selected=dataVec.selected & joinSelection)
        }

        def hasNext = {
            while (!selected.hasNext && leftIter.hasNext) {
                dataVec = leftIter.next
                selected = dataVec.selected.iterator
            }

            if (selected.hasNext) true else false
        }
    }
}
