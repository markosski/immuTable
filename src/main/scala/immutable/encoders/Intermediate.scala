package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}
import java.nio.ByteBuffer

import immutable.{Config, SourceCSV, Table}
import immutable.LoggerHelper._
import immutable.helpers.Conversions
import immutable._


/**
  * Created by marcin on 2/26/16.
  */

case class Intermediate[A](col: Column[A], table: Table) extends Encoder(col, table) with Iterable[(Int, A)] {
    def iterator = new IntermediateIterator()

    class IntermediateIterator(seek: Int=0) extends Iterator[(Int, A)] {
        val file = BufferManager.get(s"${col.name}_0")
        seek(seek)
        var counter = 0
        val colSize = col match {
            case x: VarCharColumn => 4
            case _ => col.size
        }
        var bytes = new Array[Byte](colSize)
        var oid = new Array[Byte](4)
        def next = {
            file.get(oid)
            file.get(bytes)
            counter += 1
            (
                Conversions.bytesToInt(oid),
                col match {
                    case x: VarCharColumn => Conversions.bytesToInt(bytes).asInstanceOf[A]
                    case _ => col.bytesToValue(bytes)
                }
            )
        }

        def hasNext = {
            if (counter < file.limit / (4 + colSize)) true else false
        }

        def seek(loc: Int) = {
            file.position(loc * (4 + colSize))
            counter = loc
        }
    }

    def encode(buffer: ByteBuffer): Unit = {
        val interFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${table.name}/${col.name}_0.inter", false),
            Config.readBufferSize)

        val iter = Encoder.getColumnIterator(col, table)
        var tuple = iter.next

        while(buffer.position < buffer.limit) {
            val oid = buffer.getInt

            while(iter.hasNext && oid >= tuple._1) {
                if (oid == tuple._1) {
                    interFile.write(Conversions.intToBytes(tuple._1))
                    val value = col match {
                        case x: VarCharColumn => Conversions.intToBytes(tuple._2.toString.toInt)
                        case _ => col.stringToBytes(tuple._2.toString)
                    }
                    interFile.write(value)
                }
                tuple = iter.next
            }
        }
        interFile.close
    }
}

