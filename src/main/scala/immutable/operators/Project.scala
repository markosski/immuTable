package immutable.operators

import java.nio.{IntBuffer}
import immutable.LoggerHelper._
import immutable._

import scala.collection.mutable

/**
  * Created by marcin on 3/7/16.
  */

case class ProjectPass(limit: Option[Int] = None) extends ProjectionOperator {
    def iterator = new ProjectPassIterator()

    class ProjectPassIterator extends Iterator[Seq[Any]] {
        def next = ???
        def hasNext = ???
    }
}

case class Project(cols: List[Column], op: SelectionOperator, limit: Option[Int] = None) extends ProjectionOperator {
    debug("Enter Project")
    val opIter = op.iterator
    def iterator = new ProjectIterator()

    class ProjectIterator extends Iterator[Seq[Any]] {
        var oids = IntBuffer.allocate(Config.vectorSize)
        if (opIter.hasNext)
            oids = opIter.next

        // TODO: Prevent from lookups to the same column if specified more than once.
        val encIters = cols.map(x => {
            SelectionOperator.prepareBuffer(
                x,
                SchemaManager.getTable(x.tblName)
            )
            x.getIterator
        })

        def next = {
            if (!oids.hasRemaining && opIter.hasNext)
                oids = opIter.next

            val oid = oids.get

            encIters.map(x => {
                x.seek(oid)
                x.next._2
            })
        }

        def hasNext = if (oids.hasRemaining || opIter.hasNext) true else false
    }
}

case class ProjectAggregate(cols: List[Column] = List(), aggrs: List[Aggregator], op: SelectionOperator, groupBy: Option[Column] = None) extends ProjectionOperator {
    def iterator = groupBy match {
        case Some(x) => new ProjectAggregateGroupByIterator()
        case _ => new ProjectAggregateIterator()
    }

    class ProjectAggregateIterator extends Iterator[Seq[Any]] {
        val opIter = op.iterator
        var oids = IntBuffer.allocate(Config.vectorSize)
        if (opIter.hasNext)
            oids = opIter.next

        val encIters = aggrs.map(x => {
            SelectionOperator.prepareBuffer(x.col, SchemaManager.getTable(x.col.tblName))
            x.col.getIterator
        })

        def next = {
            while (oids.hasRemaining) {
                val oid = oids.get

                if (!oids.hasRemaining && opIter.hasNext)
                    oids = opIter.next

                for (i <- 0 until aggrs.size) {
                    encIters(i).seek(oid)
                    val tuple = encIters(i).next
                    val aggr = aggrs(i)

                    aggr.add(tuple._2.asInstanceOf[aggr.T])
                }

                if (!oids.hasRemaining) oids = opIter.next
            }

            aggrs.map(x => x.get.asInstanceOf[Any])
        }

        def hasNext = if (oids.hasRemaining) true else false
    }

    class ProjectAggregateGroupByIterator extends Iterator[Seq[Any]] {
        val col = groupBy.get
        SelectionOperator.prepareBuffer(col, SchemaManager.getTable(col.tblName))

        val encIter = col.getIterator
        val opIter = op.iterator
        val groupMap = mutable.HashMap[col.DataType, List[Int]]()
        var oids = IntBuffer.allocate(Config.vectorSize)
        if (op.iterator.hasNext)
            oids = opIter.next

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

            if (!oids.hasRemaining && opIter.hasNext)
                oids = opIter.next
        }

        val mapIter = groupMap.iterator

        def next = {
            val item = mapIter.next

            aggrs.foreach(_.reset) // reset aggregators

            for (oid <- item._2; i <- 0 until aggrs.size) {
                encIters(i).seek(oid)
                val tuple = encIters(i).next
                val aggr = aggrs(i)

                aggr.add(tuple._2.asInstanceOf[aggr.T])
            }

            item._1 :: aggrs.map(x => x.get.asInstanceOf[Any])
        }

        def hasNext = if (mapIter.hasNext) true else false
    }
}
