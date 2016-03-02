package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}

import immutable.{Config, SourceCSV, Table}
import immutable.helpers.Conversions
import immutable.LoggerHelper._
import immutable._
import scala.io.Source


/**
  * Created by marcin on 2/26/16.
  */

case class RunLength[A](col: Column[A], table: Table) extends Encoder(col, table) with CSVEncoder with Iterable[(Int, A)] {
    private val repeatValueSize = 2

    class FixedRunLengthIterator[A](col: Column[A], seek: Int=0) extends Iterator[(Int, A)] {
        val rlnFile = BufferManager.get(col.name)
        seek(seek)

        val bytes: Array[Byte] = new Array[Byte](col.size + repeatValueSize) // column size + 1 byte for repeat value
        var repeat: Int = 0
        var currentValue: A = col.bytesToValue(new Array[Byte](col.size))
        var counter: Int = 0
        def next = {
            if (repeat == 0) {
                rlnFile.get(bytes)
                currentValue = col.bytesToValue(bytes.slice(0, col.size))
                repeat = Conversions.bytesToInt(bytes.slice(col.size,col.size + repeatValueSize))
            } else {
                repeat -= 1
            }

            counter += 1
            (counter - 1, currentValue)
        }

        def hasNext = {
            if (counter < table.size) true
            else false
        }

        def seek(loc: Int) = {
            rlnFile.position(col.size * loc)
            counter = loc
        }
    }

    lazy val iterator = new FixedRunLengthIterator(col)

    def encode(pos: Int, csv: SourceCSV) = {
        col match {
            case col: FixedCharColumn => encodeFixedCharNumeric(pos, csv)
            case col: NumericColumn => encodeFixedCharNumeric(pos, csv)
            case _ => new Exception("This column type is not supported by RunLenght encoder.")
        }
    }

    private def encodeFixedCharNumeric(pos: Int, csv: SourceCSV) = {
        val csvFile: Source = Source.fromFile(csv.filename) // CSV file

        val colFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.rle", false),
            Config.readBufferSize)

        var parts = Array[String]()
        var repeat: Int = 0
        var lastValue = ""
        var currentValue = ""
        var counter = 0
        for (line <- csvFile.getLines()) {
            if (csv.skipRows > 0 && counter < csv.skipRows) {
                // skipping
            } else {
                parts = line.split(csv.delim)
                currentValue = parts(pos)

                if (counter - csv.skipRows == 0) {
                    repeat = 1
                } else if (currentValue == lastValue && repeat <= Short.MaxValue ) {
                    repeat += 1
                } else {
                    colFile.write(
                        col.stringToBytes(col.stringToValue(lastValue).toString) ++ Conversions.shortToBytes(repeat.toShort)
                    )
                    repeat = 1
                }

                lastValue = currentValue
            }
            counter += 1
        }
        colFile.close()
    }
}

