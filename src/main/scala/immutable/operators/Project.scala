package immutable.operators

import java.nio.{IntBuffer}
import immutable.LoggerHelper._
import immutable._

/**
  * Created by marcin on 3/7/16.
  */
case class Project(cols: List[Column], op: SelectionOperator, limit: Option[Int] = None) extends ProjectionOperator {
    debug("Enter Project")
    val iterator = new ProjectIterator()

    class ProjectIterator() extends Iterator[Seq[_]] {
        var oids = IntBuffer.allocate(Config.vectorSize)
        oids.flip

        val encIters = cols.map(x => {
            val table = SchemaManager.getTable(x.tblName)
            SelectionOperator.prepareBuffer(x, table)
            x.getIterator
        })

        def next = {
            val oid = oids.get

            encIters.map(x => {
                x.seek(oid)
                x.next._2
            })
        }

        def hasNext = {
            if (!oids.hasRemaining) oids = op.iterator.next
            if (oids.hasRemaining) true else false
        }
    }
}

//object ProjectAggr {
//    def apply(cols: List[Aggregator], oids: ByteBuffer) = {
//
//    }
//}
