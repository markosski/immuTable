package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}

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
        case col: VarCharColumn => new DenseVarCharLoader(col)
        case _ => throw new Exception("Unsupported column type for this encoder.")
    }

    // TODO: Somehow pass table size information
    def iterator(col: Column): SeekableIterator[(Int, _)] = col match {
        case col: NumericColumn => new FixedCharNumericIterator(col)
        case col: FixedCharColumn => new FixedCharNumericIterator(col)
        case col: VarCharColumn => new VarCharIterator(col)
        case _ => throw new Exception("Unsupported column type for this iterator.")
    }

    class DenseVarCharLoader(col: Column) extends Loader {
        val varFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.densevar", false),
            Config.readBufferSize)

        def load(data: Vector[String]): Unit = {
            var i = 0
            while (i < data.size) {
                val field_val = col.stringToValue(data(i))
                val itemSize = math.min(col.stringToBytes(data(i)).length, col.size)
                varFile.write(itemSize.toByte)
                varFile.write(col.stringToBytes(field_val.toString))

                i += 1
            }
        }

        def finish: Unit = {
            varFile.flush
            varFile.close
        }
    }

    class DenseLoader(col: Column) extends Loader {
        val colFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.dense", false),
            Config.readBufferSize)

        def load(data: Vector[String]): Unit = {
            var i = 0
            while (i < data.size) {
                val field_val = col.stringToBytes(data(i))
                colFile.write(field_val)
                i += 1
            }
        }
        def finish = {
            colFile.flush
            colFile.close
        }
    }

    class FixedCharNumericIterator(col: Column, seek: Int=0) extends SeekableIterator[(Int, _)] {
        val table = SchemaManager.getTable(col.tblName)
        val file = BufferManager.get(col.FQN)
        seek(seek)

        var bytes = new Array[Byte](col.size)
        var counter = 0
        def next: (Int, _) = {
            file.get(bytes)
            counter += 1
            (counter - 1, col.bytesToValue(bytes))
        }

        def hasNext = {
            if (counter < table.size) true else false
        }

        def seek(loc: Int) = {
            file.position(loc * col.size)
            counter = loc
        }
    }

    class VarCharIterator(col: Column, seek: Int=0) extends SeekableIterator[(Int, _)] {
        val table = SchemaManager.getTable(col.tblName)
        val varFile = BufferManager.get(col.FQN)

        var counter = 0
        def next: (Int, _) = {
            val byte: Byte = varFile.get()  // gets byte containing value size
            val bytes = new Array[Byte](byte)

            varFile.get(bytes)

            counter += 1
            (counter - 1, col.bytesToValue(bytes))
        }

        def hasNext = {
            if (counter < table.size) true else false
        }

        def seek(loc: Int) = {

        }
    }
}

