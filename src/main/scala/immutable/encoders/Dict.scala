package immutable.encoders

import java.io._

import immutable.helpers.Conversions
import immutable._

import scala.collection.mutable

/**
  * Created by marcin on 2/29/16.
  */
case class Dict[A](col: Column[A], table: Table)
        extends Encoder(col, table) with Loadable with Iterable[(Int, A)] {

    col match {
        case x: CharColumn => Unit
        case _ => throw new Exception("Dict encoder can only accept char data type.")
    }

    def iterator = new DictIterator(col)
    def loader = new DictLoader(col, table)

    def lookup: mutable.HashMap[A, Int] = {
        // TODO: Lookups should be cached in some global
        val dictValFile = new BufferedInputStream(
            new FileInputStream(s"${Config.home}/${table.name}/${col.name}.dictval"),
            Config.readBufferSize)

        val lookup = mutable.HashMap[A, Int]()

        val itemSize = Array[Byte](1)
        dictValFile.read(itemSize)  // gets byte containing value size
        var bytes = new Array[Byte](itemSize(0))

        var i = 0
        while (dictValFile.read(bytes) > 0) {
            lookup += (col.bytesToValue(bytes) -> i)
            i += 1

            dictValFile.read(itemSize)  // gets byte containing value size
            bytes = new Array[Byte](itemSize(0))
        }
        lookup
    }

    class DictIterator(col: Column[A], seek: Int=0) extends Iterator[(Int, A)] {
        val bofFile = BufferManager.get(col.name)
        seek(seek)

        var bytes = new Array[Byte](4)
        var counter = 0
        def next = {
            bofFile.get(bytes)
            counter += 1
            (counter - 1, Conversions.bytesToInt(bytes).asInstanceOf[A])
        }

        def hasNext = {
            if (counter < table.size) true
            else false
        }

        def seek(loc: Int) = {
            bofFile.position(loc * 4)
            counter = loc
        }
    }

    class DictLoader(col: Column[A], table: Table) extends Loader {
        val bofFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.dict", false),
            Config.readBufferSize)

        val varFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.dictval", false),
            Config.readBufferSize)

        val offsetLookup = mutable.HashMap[A, Int]()
        var bytesWritten: Int = 0

        def load(data: Vector[String]) = {
            var OID: Int = 0

            var i: Int = 0
            while (i < data.size) {
                val field_val: A = col.stringToValue(data(i))
                val itemSize = col.stringToBytes(
                    col.stringToValue(data(i)).toString
                    ).length

                if (offsetLookup.contains(field_val)) {
                    val offset: Int = offsetLookup.get(field_val).get
                    bofFile.write(Conversions.intToBytes(offset))
                } else {
                    bofFile.write(Conversions.intToBytes(bytesWritten))
                    varFile.write(itemSize.toByte)
                    varFile.write(col.stringToBytes(field_val.toString))
                    offsetLookup.put(field_val, bytesWritten)
                    bytesWritten += field_val.toString.length + 1 // + null byte
                }
                OID += 1
                i += 1
            }
        }

        def finish = {
            bofFile.flush
            bofFile.close
            varFile.flush
            varFile.close
        }
    }
}
