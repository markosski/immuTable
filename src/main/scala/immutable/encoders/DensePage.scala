package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}

import immutable._
import immutable.LoggerHelper._

/**
  * Created by marcin on 3/1/16.
  * This encoder should be only used for numeric data type.
  * As it is right now ther is no way to restrict it as user can select arbitrary encoder.
  */

case class DensePage[A](col: Column[A] with NumericColumn[A], table: Table)
        extends Encoder(col, table) with Loadable with Iterable[(Int, A)] {

    val pageSize = 512
    val effectivePageSize = pageSize + 2 * col.size

    col match {
        case x: NumericColumn[A] => Unit
        case _ => throw new Exception(s"Problem with column '${col.name}' DensePage can only accept numeric data type.")
    }

    def iterator = new DensePageIterator(col)
    def loader = new DensePageLoader(col, table)

    class DensePageIterator(col: Column[A], seek: Int=0) extends Iterator[(Int, A)] {
        val file = BufferManager.get(col.name)

        var bytes = new Array[Byte](col.size)
        var counter = 0
        var min: A = _
        var max: A = _

        def nextPage = {
        }

        def next = {
            if (file.position % pageSize + col.size * 2 == 0) {
                file.get(bytes)
                var min = col.bytesToValue(bytes)
                file.get(bytes)
                var max = col.bytesToValue(bytes)
            }

            file.get(bytes)
            counter += 1
            (counter - 1, col.bytesToValue(bytes))
        }

        def seek(pos: Int) = {
            counter = pos
            file.position(file.position + effectivePageSize)
        }

        def hasNext = {
            if (counter < table.size) true
            else false
        }
    }

    class DensePageLoader(col: Column[A] with NumericColumn[A], table: Table) extends Loader {
        implicit val ord = col.ord

        val min = col.min
        val max = col.max

        val colFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.densep", false),
            Config.readBufferSize)

        def load(data: Vector[String]) = {
            var currentSlice = 0

            while (currentSlice < data.size / pageSize) {
                val slice = data.slice(currentSlice * pageSize, pageSize + (currentSlice * pageSize))
                val sliceMin = slice.map(x => col.stringToValue(x)).min
                val sliceMax = slice.map(x => col.stringToValue(x)).max

                colFile.write(col.stringToBytes(sliceMin.toString))
                colFile.write(col.stringToBytes(sliceMax.toString))

                var i = 0
                while (i < slice.size) {
                    val field_val = col.stringToBytes(slice(i))
                    colFile.write(field_val)
                    i += 1
                }
                currentSlice += 1
            }
        }

        def finish = {
            colFile.flush
            colFile.close
        }
    }
}
