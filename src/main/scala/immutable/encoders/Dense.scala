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

case class Dense[A](col: Column[A], table: Table) extends Encoder(col, table) with CSVEncoder with Iterable[(Int, A)] {

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

    class VarCharDictIterator[A](col: Column[A], seek: Int=0) extends Iterator[(Int, A)] {
        val bofFile = BufferManager.get(col.name)
        seek(seek)

        var bytes = new Array[Byte](4)
        var counter = 0
        def next = {
            bofFile.get(bytes)
            counter += 1
            (counter - 1, Conversions.bytesToInt(bytes).asInstanceOf[A])
        }

        def hasNext = {
            if (counter < table.size) true
            else false
        }

        def seek(loc: Int) = {
            bofFile.position(loc * 4)
            counter = loc
        }
    }

    class VarCharIterator[A](col: Column[A], seek: Int=0) extends Iterator[(Int, A)] {
        val varFile = BufferManager.get(col.name)

        var bytes = new Array[Byte](4)
        var counter = 0
        def next = {
            var bytes = Array[Byte]()
            var byte: Byte = varFile.get()  // gets byte

            while (byte.toInt != 0) {
                bytes = bytes ++ Array[Byte](byte)
                byte = varFile.get()  // gets byte
            }

            counter += 1
            (counter - 1, col.bytesToValue(bytes))
        }

        def hasNext = {
            if (counter < table.size) true
            else false
        }
    }

    lazy val iterator = col match {
        case col: VarCharColumn => new VarCharDictIterator(col)
        case col: FixedCharColumn => new FixedCharNumericIterator(col)
        case col: NumericColumn => new FixedCharNumericIterator(col)
        case _ => throw new Exception("Unknown column type")
    }

    def encode(pos: Int, csv: SourceCSV): Unit = {
        col match {
            case col: VarCharColumn => encodeVarChar(pos, csv)
            case col: FixedCharColumn => encodeFixedCharNumeric(pos, csv)
            case col: NumericColumn => encodeFixedCharNumeric(pos, csv)
        }
    }

    private def encodeFixedCharNumeric(pos: Int, csv: SourceCSV) = {
        val csvFile: Source = Source.fromFile(csv.filename) // CSV file
        val colFile = new BufferedOutputStream(
                new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.dat", false),
                Config.readBufferSize)

        var parts = Array[String]()
        var counter = 0
        info("Reading lines from csv file for encoding selected column...")
        for (line <- csvFile.getLines()) {
            if (csv.skipRows > 0 && counter < csv.skipRows) {
                info("Skipping first row of csv file.")
            } else {
                parts = line.split(csv.delim)
                val field_val = col.stringToValue(parts(pos))
                colFile.write(
                    col.stringToBytes(
                        field_val.toString
                    )
                )
            }
            counter += 1
        }
        info("...finished encoding.")
        csvFile.close()
        colFile.close()
    }

    private def encodeVarChar(pos: Int, csv: SourceCSV) = {
        val csvFile: Source = Source.fromFile(csv.filename) // CSV file
        val bofFile = new BufferedOutputStream(
                new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.bof", false),
                Config.readBufferSize)
        val varFile = new BufferedOutputStream(
            new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.var", false),
            Config.readBufferSize)

        val offsetLookup = mutable.HashMap[A, Int]()
        var OID: Int = 0
        var counter: Int = 0
        var bytesWritten: Int = 0

        for (line <- csvFile.getLines()) {
            if (csv.skipRows > 0 && counter <= csv.skipRows) {
                // skipping
            } else {
                val parts = line.split(csv.delim)
                val field_val: A = col.stringToValue(parts(pos))

                if (offsetLookup.contains(field_val)) {
                    val offset: Int = offsetLookup.get(field_val).get
                    bofFile.write(Conversions.intToBytes(offset))
                } else {
                    bofFile.write(Conversions.intToBytes(bytesWritten))
                    varFile.write(
                        col.stringToBytes(field_val.toString) ++ Array[Byte](0)
                    )
                    offsetLookup.put(field_val, bytesWritten)
                    bytesWritten += field_val.toString.length + 1 // + null byte
                }
                OID += 1
            }
            counter += 1
        }
        csvFile.close()
        bofFile.close()
        varFile.close()
    }
}

