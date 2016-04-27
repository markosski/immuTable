package immutable.operators

import java.nio.{IntBuffer}
import immutable.LoggerHelper._
import immutable._
import immutable.encoders.Dense
import immutable.helpers.Conversions

import scala.collection.mutable

/**
  * This projection operator does not take any selection operators. It will simply iterate over selected columns
  * and produce results. This would be used typically for getting N first records etc.
  * TODO: Another variation is needed for aggregated operator and groupedBy.
  *        Maybe it would be better to make Projections be more flexible instead of creating many variations.
  *
  * @param limit
  */
case class ProjectPass(limit: Option[Int] = None) extends ProjectionOperator {
    def iterator = new ProjectPassIterator()

    class ProjectPassIterator extends Iterator[Seq[Any]] {
        def next = ???
        def hasNext = ???
    }
}

/**
  * This projection operator can accept list of columns to materialize.
  * TODO: Add functionality to create in-memory tables as a result of projection operator.
  *
  * @param cols
  * @param op
  * @param limit
  */
case class Project(cols: List[Column], op: SelectionOperator, limit: Option[Int] = None) extends ProjectionOperator {
    debug("Enter Project")
    val opIter = op.iterator
    def iterator = new ProjectIterator()

    class ProjectIterator extends Iterator[Seq[Any]] {
        var dataVec = opIter.next
        var selected = dataVec.selected.iterator
        var oid = 0

        def next = {
            var record: List[Any] = List()

            while (!selected.hasNext && opIter.hasNext) {
                dataVec = opIter.next
                selected = dataVec.selected.iterator
            }

            if (selected.hasNext) {
                val localIdx = selected.next
                oid = localIdx + dataVec.vecID * Config.vectorSize

                for (i <- 0 until dataVec.data.length) {
                    record = dataVec.data(i)(localIdx) :: record
                }

                if (!selected.hasNext && opIter.hasNext) {
                    dataVec = opIter.next
                }
            }

            while (!selected.hasNext && opIter.hasNext) {
                dataVec = opIter.next
                selected = dataVec.selected.iterator
            }

            oid :: record
        }

        def hasNext = if (opIter.hasNext) true else false
    }
}

/**
  * This projection operator access columns that are used in groupBy and list of aggregators e.g. SUM, MIN, MAX.
 *
  * @param cols
  * @param aggrs
  * @param op
  * @param groupBy
  */
case class ProjectAggregate(cols: List[Column] = List(), aggrs: List[Aggregator], op: SelectionOperator, groupBy: Option[Column] = None) extends ProjectionOperator {
    def iterator = groupBy match {
        case Some(x) => new ProjectAggregateGroupByIterator()
        case _ => new ProjectAggregateIterator()
    }

    class ProjectAggregateIterator extends Iterator[Seq[Any]] {
        val opIter = op.iterator
//        val encIters = aggrs.map(x => {
//            SelectionOperator.prepareBuffer(x.col, SchemaManager.getTable(x.col.tblName))
//            x.col.getIterator
//        })
        var vecData = opIter.next
        var selection = vecData.selected.iterator
        var vecCounter = 0

        def next = {
//            while (vecData.selected.size == 0 && hasNext)
//                vecData = opIter.next
//
//            if (selection.hasNext) {
//                val selIdx = selection.next
//                for (i <- 0 until vecData.cols.size) {
//                    val tuple = vecData.cols(i).enc match {
//                        case Dense => (vecData.vecID + selIdx, vecData.cols(i).bytesToValue(vecData.data(i).slice(selIdx * 4, 4)))
//                        case _ => (vecData.vecID + selIdx, vecData.cols(i).bytesToValue(vecData.data(i).slice(selIdx * vecData.cols(i).size, vecData.cols(i).size)))
//                    }
//                    val aggr = aggrs(i)
//
//                    if (i == 1)
//                        aggr.add(tuple._2.asInstanceOf[aggr.T])
//                }
//                vecCounter += 1
//            } else if (opIter.hasNext) {
//                vecData = opIter.next
//                selection = vecData.selected.iterator
//                vecCounter += 0
//            }

            aggrs.map(x => x.get.asInstanceOf[Any])
        }

        def hasNext = if (opIter.hasNext) true else false
    }

    class ProjectAggregateGroupByIterator extends Iterator[Seq[Any]] {
        val col = groupBy.get
        SelectionOperator.prepareBuffer(col, SchemaManager.getTable(col.tblName))

        val encIter = col.getIterator
        val opIter = op.iterator
        val groupMap = mutable.HashMap[col.DataType, List[Int]]()
        var oids = IntBuffer.allocate(Config.vectorSize)
//        if (op.iterator.hasNext)
//            oids = opIter.next

        val encIters = aggrs.map(x => {
            SelectionOperator.prepareBuffer(x.col, SchemaManager.getTable(x.col.tblName))
            x.col.getIterator
        })

        // Build group HashMap that will contain unique values and list of oIDs
        while (oids.hasRemaining) {
            encIter.seek(oids.get)
            val tuple = encIter.next.asInstanceOf[(Int, col.DataType)]

            if (groupMap.contains(tuple._2)) {
                groupMap.update(tuple._2, tuple._1 :: groupMap.get(tuple._2).get)
            } else {
                groupMap.put(tuple._2, List(tuple._1))
            }

//            if (!oids.hasRemaining && opIter.hasNext)
//                oids = opIter.next
        }

        val mapIter = groupMap.iterator

        def next = {
            val item = mapIter.next

            aggrs.foreach(_.reset) // reset aggregators

            for (oid <- item._2; i <- 0 until aggrs.size) {
                encIters(i).seek(oid)
                val tuple = encIters(i).next
                val aggr = aggrs(i)

                List()
//                aggr.add(tuple._2.asInstanceOf[aggr.T])
            }

            item._1 :: aggrs.map(x => x.get.asInstanceOf[Any])
        }

        def hasNext = if (mapIter.hasNext) true else false
    }
}

/**
  * Experimental projection operator that operates on input operators in parallel.
  */
//case class ProjectConcurrent(cols: List[Column], op: SelectionOperator) extends ProjectionOperator {
//    debug("Enter ProjectConcurrent")
//
//    import scala.concurrent.ExecutionContext.Implicits.global
//    import scala.concurrent.Future
//
//    val concurrency = 4
//    val opIters = List.fill(concurrency)(op.iterator)
//    def iterator = new ProjectConcurrentIterator()
//
//    class ProjectConcurrentIterator extends Iterator[Seq[_]] {
//        var oidsPool = List.fill(concurrency)(IntBuffer.allocate(Config.vectorSize))
//        opIters.map(x => Future(x.next))
//
//
//        def next = {
//            if (!oids.hasRemaining && opIter.hasNext)
//                oids = opIter.next
//
//            val oid = oids.get
//
//            encIters.map(x => {
//                x.seek(oid)
//                x.next._2
//            })
//        }
//
//
//        def hasNext = if (oids.hasRemaining || opIter.hasNext) true else false
//    }
//}