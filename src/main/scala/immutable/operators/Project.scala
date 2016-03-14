package immutable.operators

import java.nio.ByteBuffer

import immutable.LoggerHelper._
import immutable._
import immutable.encoders.{Dict, Dense, Encoder}

/**
  * Created by marcin on 3/7/16.
  */
case class Project(cols: List[Column], oids: ByteBuffer, limit: Option[Int] = None) extends Iterable[Seq[_]] {
    debug("Enter Project")
    val iterator = new ProjectIterator()

    class ProjectIterator() extends Iterator[Seq[_]] {
        var counter = 0
        var oid: Int = 0
        var bufferSize = oids.limit/4
        val iterators = cols.map(x => {
            val table = SchemaManager.getTable(x.tblName)
            Operator.prepareBuffer(x, table)
            x.getIterator
        })

        def next = {
            oid = oids.getInt
            counter += 1
            iterators.map(x => { x.seek(oid); x.next._2 })
        }

        def hasNext = {
            if (counter < bufferSize) true else false
        }
    }
}

//object ProjectAggr {
//    def apply(cols: List[Aggregator], oids: ByteBuffer) = {
//
//    }
//}
