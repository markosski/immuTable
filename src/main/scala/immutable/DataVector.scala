package immutable

import immutable.operators.SelectionOperator

import scala.collection.mutable

/**
  * Created by marcin on 4/4/16.
  */
case class DataVector(val vecID: Int, val cols: List[Column], val data: List[Array[_]], val selected: mutable.BitSet = mutable.BitSet())

//class DataVectorProducer(table: Table, cols: List[Column]) extends Iterable[DataVector] {
//    def iterator = new DataVectorIterator()

//    class DataVectorIterator extends Iterator[DataVector] {
//        var vecCounter = -1  // because we want it to start at 0
//        val encIters = cols.map(x => {
//            SelectionOperator.prepareBuffer(
//                x,
//                SchemaManager.getTable(x.tblName)
//            )
//            x.getIterator
//        })
//
//        def next = {
//            vecCounter += 1
//            new DataVector(vecCounter, cols, encIters.map(x => x.next))
//        }
//
//        def hasNext = {
//            if (vecCounter * Config.vectorSize >= table.size) false else true
//        }
//    }
//}
