package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}

import immutable.{Config, Table}
import immutable._

/**
  * Created by marcin on 2/26/16.
  */

case object Dense extends Encoder with EncoderDescriptor {
    def loader(col: Column): Loader = new DenseLoader(col)
    def iterator(col: Column): SeekableIterator[(Int, _)] = new DenseIterator(col)
    def descriptor(col: Column) = new DenseBlockDescriptor(col)

    class DenseLoader(col: Column) extends Loader {
        val colFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.dense", false),
            Config.readBufferSize)

        def write(data: Vector[String]): Unit = {
            var i = 0
            while (i < data.size) {
                val field_val = col.stringToBytes(data(i))
                colFile.write(field_val)
                i += 1
            }
        }
        def close = {
            colFile.flush
            colFile.close
        }
    }

    class DenseIterator(col: Column, seek: Int=0) extends SeekableIterator[(Int, _)] {
        val table = SchemaManager.getTable(col.tblName)
        val file = BufferManager.get(col.FQN)

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

        def position = counter
    }

    class DenseBlockDescriptor(val col: Column) extends BlockDescriptor[NumericDescriptor[_]] {
        def add(vec: Vector[String]) = {
            col match {
                case col: NumericColumn => {
                    val hist = Stats.histogram(vec, col, 10)
                    var max = col.min
                    var min = col.max

                    vec.foreach(x => {
                        val xc = col.stringToValue(x)

                        if (col.num.lt(xc, min)) {
                            min = xc
                        } else if (col.num.gt(xc, max)) {
                            max = xc
                        }
                    })
                    descriptors = descriptors :+ NumericDescriptor[col.DataType](min, max, hist)
                }
                case _ => Unit
            }
        }
    }
}


