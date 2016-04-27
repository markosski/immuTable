package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}
import java.nio.{IntBuffer, ByteBuffer}

import immutable.{Config, Table}
import immutable.helpers.Conversions
import immutable.LoggerHelper._
import immutable._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success}

/**
  * Created by marcin on 2/26/16.
  */

case object Dense extends Encoder {
    def loader(col: Column): Loader = col match {
        case col: NumericColumn => new DenseLoader(col)
        case col: FixedCharColumn => new DenseLoader(col)
        case _ => throw new Exception("Unsupported column type for this encoder.")
    }

    def iterator(col: Column): SeekableIterator[Vector[_]] = col match {
        case col: NumericColumn => new FixedCharNumericIterator(col)
        case col: FixedCharColumn => new FixedCharNumericIterator(col)
        case _ => throw new Exception("Unsupported column type for this iterator.")
    }

    class DenseLoader(col: Column) extends Loader {
        val colFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.dense", false),
            Config.readBufferSize)

        def load(data: Vector[String]): Unit = {
            var i = 0
            while (i < data.size) {
                val field_val = data(i) match {
                    case "null" => col.stringToBytes(col.nullVal.toString)
                    case _ => col.stringToBytes(data(i))
                }

                colFile.write(field_val)
                i += 1
            }
        }
        def finish = {
            colFile.flush
            colFile.close
        }
    }

    class FixedCharNumericIterator(val col: Column, seek: Int=0) extends SeekableIterator[Vector[_]] {
        val table = SchemaManager.getTable(col.tblName)
        val file = BufferManager.get(col.FQN)

        var counter = 0

        def next = {
            val bytes = new Array[Byte](col.size)
            var vec = Vector[col.DataType]()

            val vecSize = if (file.remaining > Config.vectorSize * col.size) Config.vectorSize else (file.remaining / col.size)

            for (i <- 0 until vecSize) {
                file.get(bytes)
                vec = vec :+ col.bytesToValue(bytes)
                counter += file.position / col.size
            }
            vec
        }

        def hasNext = {
            if (counter < file.limit / col.size) true else false
        }

        def seek(loc: Int) = {
            file.position(loc * col.size)
            counter = loc
        }
    }
}

