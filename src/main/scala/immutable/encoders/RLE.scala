package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}

import immutable.{Config, Table}
import immutable.helpers.Conversions
import immutable.LoggerHelper._
import immutable._
import scala.io.Source


/**
  * Created by marcin on 2/26/16.
  */

case object RLE extends Encoder {
    private val repeatValueSize = 2

    def iterator(col: Column) = new FixedRunLengthIterator(col)
    def loader(col: Column) = new RunLengthLoader(col)

    class FixedRunLengthIterator(col: Column, seek: Int=0) extends SeekableIterator[(Int, _)] {
        val rlnFile = BufferManager.get(col.name)
        seek(seek)

        val bytes: Array[Byte] = new Array[Byte](col.size + repeatValueSize) // column size + 1 byte for repeat value
        var repeat: Int = 0
//        var currentValue = col.bytesToValue(new Array[Byte](col.size))
        var counter: Int = 0
        def next: (Int, _) = {
            if (repeat == 0) {
                rlnFile.get(bytes)
//                currentValue = col.bytesToValue(bytes.slice(0, col.size))
                repeat = Conversions.bytesToInt(bytes.slice(col.size,col.size + repeatValueSize))
            } else {
                repeat -= 1
            }

            counter += 1
            (counter - 1, col.bytesToValue(bytes.slice(0, col.size)))
        }

        // TODO: need table size info
        def hasNext = {
            if (counter < 1000000) true else false
        }

        def seek(loc: Int) = {
            rlnFile.position(col.size * loc)
            counter = loc
        }

        def position = counter
    }

    class RunLengthLoader(col: Column) extends Loader {
        val colFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.rle", false),
            Config.readBufferSize)

        def write(data: Vector[String]) = {
            var repeat: Int = 0
            var lastValue = ""
            var i = 0

            while (i < data.size) {
                if (data(i) == lastValue && repeat <= Short.MaxValue ) {
                    repeat += 1
                } else {
                    colFile.write(
                        col.stringToBytes(col.stringToValue(lastValue).toString) ++ Conversions.shortToBytes(repeat.toShort)
                    )
                    repeat = 1
                }

                lastValue = data(i)
                i += 1
            }
        }

        def close = {
            colFile.close()
        }
    }
}

