package immutable.operators

import immutable.{DataVector, Aggregator}
import immutable.LoggerHelper._

/**
  * Created by marcin on 5/18/16.
  */
case class Aggregate(aggrs: List[Aggregator], flow: SelectionOperator) extends ProjectionOperator {
    debug("Enter ProjectAggregate")
    def iterator = new ProjectAggregateIterator()

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
        val flowIter = flow.iterator
        var dataVec = flowIter.next
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

                while (!selected.hasNext && flowIter.hasNext) {
                    dataVec = flowIter.next
                    selected = dataVec.selected.iterator
                }
            }

            aggrs.map(x => x.get.asInstanceOf[Any])
        }

        def hasNext = {
            while (!selected.hasNext && flowIter.hasNext) {
                dataVec = flowIter.next
                selected = dataVec.selected.iterator
            }

            if (selected.hasNext) true else false
        }
    }
}
