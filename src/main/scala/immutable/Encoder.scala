/**
  * Created by marcin on 2/4/16.
  */
package main.scala.immutable

import java.io.{RandomAccessFile, BufferedOutputStream, FileOutputStream}
import scala.collection.mutable.HashMap
import scala.io.Source
import helpers.Conversions
import immutable.LoggerHelper._
import immutable.SourceCSV
import scala.util.Random

abstract class ColumnStats[A](sampleSize: Int, cardinality: Int)
case class StringColumnStats[A](sampleSize: Int, cardinality: Int) extends ColumnStats[A](sampleSize, cardinality)
case class NumericColumnStats[A](cardinality: Int, sampleSize: Int, min: A, max: A) extends ColumnStats[A](sampleSize, cardinality)

abstract class Encoder[A](col: Column[A], table: String) {
    def encode(pos: Int, csv: SourceCSV): Unit
    def createStats(pos: Int, csv: SourceCSV): ColumnStats[A] = {
        import col.ord

        val sampleThresh = 1000
        val sampleSize = 0.01
        var rows = 0

        val csvFile = new RandomAccessFile(csv.filename, "r")
        val csvLength = csvFile.length.toInt

        // TODO: this is wrong!
        if (csvFile.length / col.size * sampleSize < sampleThresh) rows = sampleThresh
        else rows = (csvFile.length / col.size * sampleSize).toInt

        val values = HashMap[A, Int]()
        for (i <- 0 until rows) {
            var randBytePos = Random.nextInt(csvLength).toLong
            csvFile.seek(randBytePos)

            var nextChar = csvFile.readByte()
            while (nextChar != 10.toByte) {
                nextChar = csvFile.readByte()
            }
            var line = csvFile.readLine()
            var parts = line.split(csv.delim)
            var value = col.stringToValue(parts(pos))

            if (values.contains(value))
                values.put(value, values.get(value).get + 1)
            else
                values.put(value, 1)
        }
        csvFile.close()

        col match {
            case x: NumericColumn => NumericColumnStats(rows, values.size, values.min._1, values.max._1)
            case x: CharColumn => StringColumnStats(rows, values.size)
        }
    }
}

case class Dense[A](col: Column[A], table: String) extends Encoder(col, table) with Iterable[(Int, A)] {

    class FixedCharNumericIterator[A](col: Column[A]) extends Iterator[(Int, A)] {
        val file = new RandomAccessFile(s"/Users/marcin/correla/$table%s/${col.name}%s.dat", "r")
        val fileSize = file.length
        var bytes = new Array[Byte](col.size)
        var counter = 0
        def next = {
            file.read(bytes)
            counter += 1
            (counter - 1, col.bytesToValue(bytes))
        }

        def hasNext = {
            if (counter * col.size >= fileSize) {
                file.close
                false
            }
            else true
        }
    }

    class VarCharIterator[A](col: Column[A]) extends Iterator[(Int, A)] {
        val bofFile = new RandomAccessFile(s"/Users/marcin/correla/$table%s/${col.name}%s.bof", "r")
        val varFile = new RandomAccessFile(s"/Users/marcin/correla/$table%s/${col.name}%s.var", "r")
        val bofFileSize = bofFile.length

        var counter = 0
        var offsetBytes = new Array[Byte](4)
        def next = {
            varFile.seek(Conversions.bytesToInt(offsetBytes))
            var bytes = Array[Byte]()
            var byte: Byte = varFile.readByte()

            while (byte.toInt != 0) {
                bytes = bytes ++ Array[Byte](byte)
                byte = varFile.readByte()
            }

            counter += 1
            (counter - 1, col.bytesToValue(bytes))
        }

        def hasNext = {
            if (counter * 4 >= bofFileSize) {
                bofFile.close
                varFile.close
                false
            }
            else true
        }
    }

    lazy val iterator = col match {
        case col: VarCharColumn => new VarCharIterator(col)
        case col: FixedCharColumn => new FixedCharNumericIterator(col)
        case col: NumericColumn => new FixedCharNumericIterator(col)
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
            new FileOutputStream(s"/Users/marcin/correla/$table%s/${col.name}%s.dat", false),
            4096)

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
                new FileOutputStream(s"/Users/marcin/correla/$table%s/${col.name}%s.bof", false),
                4096)
        val varFile = new BufferedOutputStream(
            new FileOutputStream(s"/Users/marcin/correla/$table%s/${col.name}%s.var", false), 4096)

        val offsetLookup = HashMap[A, Int]()
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

case class RunLength[A](col: Column[A], table: String) extends Encoder(col, table) with Iterable[(Int, A)] {
    private val repeatValueSize = 2

    class FixedRunLengthIterator[A](col: Column[A]) extends Iterator[(Int, A)] {
        // TODO: need to get number of records
        val rlnFile = new RandomAccessFile(s"/Users/marcin/correla/$table%s/${col.name}%s.rle", "r")
        val recordSize = 2000

        var bytes: Array[Byte] = new Array[Byte](col.size + repeatValueSize) // column size + 1 byte for repeat value
        var repeat: Int = 0
        var currentValue: A = col.bytesToValue(Array[Byte]())
        var counter: Int = 0
        def next = {
            if (repeat == 0) {
                rlnFile.read(bytes)
                currentValue = col.bytesToValue(bytes.slice(0, col.size))
                repeat = Conversions.bytesToInt(bytes.slice(col.size,col.size + repeatValueSize))
            }
            repeat -= 1
            counter += 1
            (counter - 1, currentValue)
        }

        def hasNext = {
            if (counter <= recordSize) true
            else {
                rlnFile.close
                false
            }
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
            new FileOutputStream(s"/Users/marcin/correla/$table%s/${col.name}%s.rle", false),
            4096)

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

object EncoderMain extends App {
    info("Start...")

    val col = FixedCharColumn("state", 2)
    val iter = Dense(col, "correla_dataset_small").iterator
    for (i <- 0 until 10)
        println(iter.next)
}