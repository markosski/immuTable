package immutable

import immutable.operators.SelectionOperator

import scala.collection.mutable

/**
  * Created by marcin on 4/4/16.
  */
class DataVector(val vecID: Int, val cols: List[Column], val data: Seq[Array[Byte]]) {
    var selected = mutable.BitSet()
    for (i <- 0 until Config.vectorSize) selected.add(i)
}

class DataVectorProducer(table: Table, cols: List[Column]) extends Iterable[DataVector] {
    def iterator = new DataVectorIterator()

    class DataVectorIterator extends Iterator[DataVector] {
        var vecCounter = 0
        val encIters = cols.map(x => {
            SelectionOperator.prepareBuffer(
                x,
                SchemaManager.getTable(x.tblName)
            )
            x.getIterator
        })

        def next = {
            vecCounter += 1
            new DataVector(vecCounter, cols, encIters.map(x => x.next))
        }

        def hasNext = {
            if (vecCounter * Config.vectorSize >= table.size) false else true
        }
    }
}
