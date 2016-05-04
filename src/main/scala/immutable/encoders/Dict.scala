package immutable.encoders

import java.nio.{ByteBuffer, IntBuffer}

import scala.collection.mutable
import java.io._

import immutable.helpers.Conversions
import immutable.LoggerHelper._
import immutable._

/**
  * Created by marcin on 2/29/16.
  */

case object Dict extends Encoder {
    def iterator(col: Column) = new DictIterator(col)
    def loader(col: Column) = new DictLoader(col)
    val lookups = mutable.Map[String, mutable.HashMap[_, Int]]()

    def lookup(col: Column): mutable.HashMap[col.DataType, Int] = {
        if (lookups.contains(col.FQN)) lookups.get(col.FQN).get.asInstanceOf[mutable.HashMap[col.DataType, Int]]
        else {
            info(s"Creating lookup for ${col.name}")
            val dictValFile = new BufferedInputStream(
                new FileInputStream(s"${Config.home}/${col.tblName}/${col.name}.dictval"),
                Config.readBufferSize)

            val lookup = mutable.HashMap[col.DataType, Int]()

            val itemSize = Array[Byte](1)
            dictValFile.read(itemSize)  // gets byte containing value size
            var bytes = new Array[Byte](itemSize(0))
            var bytesRead: Int = 0

            var i = 0
            while (dictValFile.read(bytes) > 0) {
                lookup += (col.bytesToValue(bytes) -> bytesRead)
                i += 1

                dictValFile.read(itemSize)  // gets byte containing value size

                bytesRead += bytes.length + 1 // Mark byte position of current item (+1 null byte)

                bytes = new Array[Byte](itemSize(0))
            }
            info(s"Finished creating lookup for ${col.name} with ${lookup.size} entries.")
            lookups.put(col.FQN, lookup)
            dictValFile.close
            lookup
        }
    }

    class DictIterator(val col: Column) extends SeekableIterator[Int] {
        val table = SchemaManager.getTable(col.tblName)
        val file = ColumnBufferManager.get(col.FQN)
        val valSize = 4

        var counter = 0

        def next = {
            counter += 1
            val bytes = new Array[Byte](valSize)
            file.get(bytes)
            Conversions.bytesToInt(bytes)
        }

        def hasNext = {
            if (counter * valSize < file.limit) true else false
        }

        def seek(loc: Int) = {
            file.position(loc * valSize)
            counter = loc
        }

        def position = counter
    }

    case class DictLoader(col: Column) extends Loader {
        val bofFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.dict", false),
            Config.readBufferSize)

        val varFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.dictval", false),
            Config.readBufferSize)

        val offsetLookup = mutable.HashMap[col.DataType, Int]()
        var bytesWritten: Int = 0

        def write(data: Vector[String]) = {
            var OID: Int = 0

            var i: Int = 0
            while (i < data.size) {
                val field_val: col.DataType = data(i) match {
                    case "null" => col.nullVal
                    case _ => col.stringToValue(data(i))
                }
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

        def close = {
            bofFile.flush
            bofFile.close
            varFile.flush
            varFile.close
        }
    }
}
