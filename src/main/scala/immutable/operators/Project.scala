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

    def resolveDataVecColIdx(dataVec: DataVector) = {
        var colIdxLookup = Map[String, Int]()

        for (i <- 0 until cols.size) {
            dataVec.cols.map(x => x.name).zipWithIndex.find(x => x._1 == cols(i).name) match {
                case Some(x) => colIdxLookup = colIdxLookup + x
                case _ => throw new Exception("Could not match column in resolveDataVecColIdx")
            }
        }
        colIdxLookup
    }

    class ProjectIterator extends Iterator[Seq[Any]] {
        var dataVec = opIter.next
        var selected = dataVec.selected.iterator
        val dataVecColIdx = resolveDataVecColIdx(dataVec)

        def next = {
            var record: List[Any] = List()
            val localIdx = selected.next
            val oid = localIdx + dataVec.vecID - dataVec.data(0).size

            for (i <- 0 until cols.size) {
                record = record :+ dataVec.data(dataVecColIdx.get(cols(i).name).get)(localIdx)
            }

            record
        }

        def hasNext = {
            while (!selected.hasNext && opIter.hasNext) {
                dataVec = opIter.next
                selected = dataVec.selected.iterator
            }

            if (selected.hasNext) true else false
        }
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
    debug("Enter ProjectAggregate")
    def iterator = groupBy match {
        case Some(x) => new ProjectAggregateGroupByIterator()
        case _ => new ProjectAggregateIterator()
    }

    def resolveDataVecColIdx(dataVec: DataVector) = {
        var colIdxLookup = Map[String, Int]()

        for (i <- 0 until aggrs.size) {
            dataVec.cols.map(x => x.name).zipWithIndex.find(x => x._1 == aggrs(i).col.name) match {
                case Some(x) => colIdxLookup = colIdxLookup + x
                case _ => throw new Exception("Could not match column in resolveDataVecColIdx")
            }
        }
        colIdxLookup
    }

    class ProjectAggregateIterator extends Iterator[Seq[Any]] {
        val opIter = op.iterator
        var dataVec = opIter.next
        var selected = dataVec.selected.iterator
        val dataVecColIdx = resolveDataVecColIdx(dataVec)

        def next = {
            while (selected.hasNext) {
                val localIdx = selected.next

                for (i <- 0 until aggrs.size) {
                    val aggr = aggrs(i)
                    val item = dataVec.data(dataVecColIdx.get(aggr.col.name).get)(localIdx).asInstanceOf[aggr.col.DataType]
                    aggr.add(item.asInstanceOf[aggr.T])
                }

                while (!selected.hasNext && opIter.hasNext) {
                    dataVec = opIter.next
                    selected = dataVec.selected.iterator
                }
            }

            aggrs.map(x => x.get.asInstanceOf[Any])
        }

        def hasNext = {
            while (!selected.hasNext && opIter.hasNext) {
                dataVec = opIter.next
                selected = dataVec.selected.iterator
            }

            if (selected.hasNext) true else false
        }
    }

    class ProjectAggregateGroupByIterator extends Iterator[Seq[Any]] {
        // Group we are grouping by
        val groupCol = groupBy.get

        // Operator from child dataflow
        val opIter = op.iterator

        // Storage for grouped results
        val groupMap = mutable.HashMap[groupCol.DataType, List[Aggregator]]()

        // Current vector
        var dataVec = opIter.next

        // Selected tuples in current vector
        var selected = dataVec.selected.iterator

        // Resolver for proper data column in vector
        val dataVecColIdx = resolveDataVecColIdx(dataVec)

        // Get index of group column in vector columns
        val colIdx = dataVec.cols.zipWithIndex.find(x => x._1.name == groupCol.name).get._2

        while (!selected.hasNext && opIter.hasNext) {
            dataVec = opIter.next
            selected = dataVec.selected.iterator
        }

        // Build group HashMap that will contain unique values and list of oIDs
        while (selected.hasNext) {
            val localIdx = selected.next

            // TODO: Use List of items to make it multi-grouping
            val item = dataVec.data(colIdx)(localIdx).asInstanceOf[groupCol.DataType]

            if (groupMap.contains(item)) {
                for (i <- 0 until aggrs.size) {
                    val groupAggrs = groupMap.get(item).get
                    val aggr = groupAggrs(i)
                    val groupItem = dataVec.data(dataVecColIdx.get(aggr.col.name).get)(localIdx).asInstanceOf[aggr.col.DataType]
                    aggr.add(groupItem.asInstanceOf[aggr.T])
                }
            } else {
                val groupAggrs = aggrs.map(x => {
                    x match {
                        case aggr: Max => aggr.copy()
                        case aggr: Min => aggr.copy()
                        case aggr: Avg => aggr.copy()
                        case aggr: Count => aggr.copy()
                        case _ => throw new Exception("Aggregator not recognized.")
                    }
                })

                for (i <- 0 until aggrs.size) {
                    val aggr = groupAggrs(i)
                    val groupItem = dataVec.data(dataVecColIdx.get(aggr.col.name).get)(localIdx).asInstanceOf[aggr.col.DataType]
                    aggr.add(groupItem.asInstanceOf[aggr.T])
                }

                groupMap.put(item, groupAggrs)
            }

            while (!selected.hasNext && opIter.hasNext) {
                dataVec = opIter.next
                selected = dataVec.selected.iterator
            }
        }

        val mapIter = groupMap.iterator

        override def toString = ""

        def next = {
            val groupItem = mapIter.next
            groupItem._1 :: groupItem._2.map(x => x.get.asInstanceOf[Any])
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