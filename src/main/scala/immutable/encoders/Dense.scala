package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}

import immutable.{Config, SourceCSV, Table}
import immutable.helpers.Conversions
import immutable.LoggerHelper._
import immutable._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success}

/**
  * Created by marcin on 2/26/16.
  */

case class Dense[A](col: Column[A], table: Table)
        extends Encoder(col, table) with Loadable with Iterable[(Int, _)] {

    def loader = col match {
        case col: NumericColumn => new DenseLoader()
        case col: FixedCharColumn => new DenseLoader()
        case col: VarCharColumn => new DenseVarCharLoader()
        case _ => throw new Exception("Unsupported column type for this encoder.")
    }

//    def iterator = col match {
//        case col: NumericColumn[A] => new FixedCharNumericIterator()
//        case col: FixedCharColumn => new FixedCharNumericIterator()
//        case col: VarCharColumn => new VarCharIterator()
//        case _ => throw new Exception("Unsupported column type for this iterator.")
//    }

    def iterator = new FixedCharNumericIterator()

    class DenseVarCharLoader() extends Loader {
        val varFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.densevar", false),
            Config.readBufferSize)

        def load(data: Vector[String]): Unit = {
            var i = 0
            while (i < data.size) {
                val field_val: A = col.stringToValue(data(i))
                val itemSize = math.min(col.stringToBytes(data(i)).length, col.size)
                varFile.write(itemSize.toByte)
                varFile.write(col.stringToBytes(field_val.toString))

                i += 1
            }
        }

        def finish: Unit = {
            varFile.close()
        }
    }

    class DenseLoader() extends Loader {
        val colFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.dense", false),
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
            colFile.close
        }
    }

    class FixedCharNumericIterator(seek: Int=0) extends Iterator[(Int, _)] with SeekableIterator {
        val file = BufferManager.get(col.name)
        seek(seek)

        var bytes = new Array[Byte](col.size)
        var counter = 0
        def next = {
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

    class VarCharIterator(seek: Int=0) extends Iterator[(Int, _)] with SeekableIterator {
        val varFile = BufferManager.get(col.name)

        var counter = 0
        def next = {
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

