package immutable.encoders

import java.io.RandomAccessFile
import java.nio.ByteBuffer

import immutable._
import immutable.{NumericColumn}
import immutable.helpers._

import scala.collection.mutable.HashMap
import scala.util.Random

import immutable.LoggerHelper._

/**
  * Created by marcin on 2/26/16.
  */

trait Loader {
    def load(data: Vector[String]): Unit
    def finish: Unit
}

trait Loadable {
    def loader: Loader
}

trait SeekableIterator {
    def seek(pos: Int): Unit
}

abstract class Encoder[A](col: Column[A], table: Table) {

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
            val line = csvFile.readLine()
            val parts = line.split(csv.delim)
            val value = col.stringToValue(parts(pos))

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

object Encoder {
    def getColumnIterator[A](col: Column[A], table: Table): Iterator[(Int, _)] = {
        col.encoder match {
            case 'RunLength => RunLength(col, table).iterator
            case 'Dense => Dense(col, table).iterator
            case 'Dict => Dict(col, table).iterator
            case 'Intermediate => Intermediate(col, table).iterator
            case _ => throw new Exception("Unknown encoder!")
        }
    }
}


object EncoderMain extends App {
    import java.util.Date
    info("Start...")
    val start = new Date().getTime()

    val table = "correla_dataset_small"
    val col = TinyIntColumn("age", encoder='RunLength)
    val repeatValueSize = 2

    val rlnFile = new RandomAccessFile(s"${Config.home}/$table/${col.name}.rle", "r")
    val recordSize = 1000000

    var bytes: Array[Byte] = new Array[Byte](col.size + repeatValueSize) // column size + 1 byte for repeat value
    var repeat: Int = 0
    var currentValue = col.bytesToValue(new Array[Byte](col.size))
    var counter: Int = 0

    while (counter <= recordSize) {
        rlnFile.read(bytes)
        currentValue = col.bytesToValue(bytes.slice(0, col.size))
        repeat = Conversions.bytesToInt(bytes.slice(col.size,col.size + repeatValueSize))

        if (repeat > 0) {
            repeat -= 1
            counter += 1
        }

        //        (counter - 1, currentValue)
    }
    println("All completed in: " + (new Date().getTime() - start).toString + "ms")
    rlnFile.close()

    //    val table = Table(
    //        "correla_dataset_small",
    //        TinyIntColumn("age", encoder='RunLength),
    //        VarCharColumn("fname", 15),
    //        VarCharColumn("lname", 30),
    //        VarCharColumn("city", 30),
    //        FixedCharColumn("state", 2, encoder='RunLength),
    //        FixedCharColumn("zip", 5),
    //        TinyIntColumn("score1"),
    //        TinyIntColumn("score2"),
    //        TinyIntColumn("score3"),
    //        TinyIntColumn("score4")
    //    )
    //
    //    val iter0 = Encoder.getColumnIterator(table.columns(0), table.name)
    //    val iter1 = Encoder.getColumnIterator(table.columns(1), table.name)
    //    val iter2 = Encoder.getColumnIterator(table.columns(2), table.name)
    //    val iter4 = Encoder.getColumnIterator(table.columns(4), table.name)
    //    for (i <- 0 until 100)
    //        println(s"${iter0.next._2} ${iter1.next._2} ${iter2.next._2} ${iter4.next._2}")
}

