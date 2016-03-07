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

case class Dense[A](col: Column[A], table: Table) extends Encoder(col, table) with Iterable[(Int, A)] {
    def loader = col match {
        case col: NumericColumn[A] => new DenseLoader(col, table)
        case col: FixedCharColumn => new DenseLoader(col, table)
        case col: VarCharColumn => new DenseVarCharLoader(col, table)
        case _ => throw new Exception("Unsupported column type for this encoder.")
    }

    def iterator = col match {
        case col: NumericColumn[A] => new FixedCharNumericIterator[A](col)
        case col: FixedCharColumn => new FixedCharNumericIterator[A](col)
        case col: VarCharColumn => new VarCharIterator[A](col)
        case _ => throw new Exception("Unsupported column type for this iterator.")
    }

    class DenseVarCharLoader(col: Column[A], table: Table) extends Loader {
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

    class DenseLoader(col: Column[A], table: Table) extends Loader {
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

    class FixedCharNumericIterator[A](col: Column[A], seek: Int=0) extends Iterator[(Int, A)] {
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
            if (counter < table.size) true
            else false
        }

        def seek(loc: Int) = {
            file.position(loc * col.size)
            counter = loc
        }
    }

    class VarCharIterator[A](col: Column[A], seek: Int=0) extends Iterator[(Int, A)] {
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
            if (counter < table.size) true
            else false
        }
    }
}

