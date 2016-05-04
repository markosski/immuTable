package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}
import java.nio.{ByteBuffer, IntBuffer}

import immutable.{Config, Table}
import immutable.helpers.Conversions
import immutable.LoggerHelper._
import immutable._
import scala.io.Source


/**
  * Created by marcin on 2/26/16.
  */

case object RLE extends Encoder {
    val repeatValueSize = 2
    val pageSize = 1024 // how many items

    def iterator(col: Column) = new RLEIterator(col)
    def loader(col: Column) = new RLELoader(col)

    class RLEIterator(val col: Column) extends SeekableIterator[Any] {
        val table = SchemaManager.getTable(col.tblName)
        val rlnFile = ColumnBufferManager.get(col.name)
        val byteOffset = (col.size + repeatValueSize) * pageSize

        val bytes: Array[Byte] = new Array[Byte](col.size + repeatValueSize) // column size + 2 byte for repeat value
        var repeat: Int = 0
        var counter: Int = 0
        var delta: Int = 0

        def next = {
            // val vec = Vector[(Int, _)]()

//            while (delta > 0) {
//                if (repeat == 0) {
//                    rlnFile.get(bytes)
//                    repeat = Conversions.bytesToInt(bytes.slice(col.size,col.size + repeatValueSize))
//                } else {
//                    repeat -= 1
//                }
//
//                counter += 1
//                delta -= 1
//            }
//
//            if (delta == 0) rlnFile.get(bytes)
//
//            (counter - 1, col.bytesToValue(bytes.slice(0, col.size)))
//            bytes
            col.bytesToValue(new Array[Byte](0))
        }

        def hasNext = {
            if (counter < table.size) true else false
        }

        def seek(loc: Int) = {
            counter = loc / pageSize * pageSize
            delta = loc - (loc / pageSize) * pageSize
        }

        def position = 0
    }

    class RLELoader(col: Column) extends Loader {
        val colFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.rle", false),
            Config.readBufferSize)

        def write(data: Vector[String]) = {
            var repeat: Int = 0
            var lastValue = data(0) match {
                case "null" => col.nullVal.toString
                case _ => data(0)
            }
            var i = 1

            while (i < data.size) {

                if (data(i) == lastValue && repeat <= Short.MaxValue) {
                    repeat += 1
                } else {
                    colFile.write(
                        col.stringToBytes(lastValue) ++ Conversions.shortToBytes(repeat.toShort)
                    )

                    lastValue = data(i) match {
                        case "null" => col.nullVal.toString
                        case _ => data(i)
                    }
                    repeat = 1
                }

                i += 1
            }
        }

        def close = {
            colFile.flush
            colFile.close
        }
    }
}

