package immutable

import java.io.{File, FileWriter, FileOutputStream}

import immutable.encoders.EncoderDescriptor
import immutable.encoders.Encoder
import immutable.encoders._
import scala.collection.mutable
import scala.io.Source
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/9/16.
  */

case class SourceCSV(filename: String, delim: Char, skipHeader: Boolean = false, enclosed: Option[Char] = None, skipRows: Int = 0)

object DataLoader {
    def fromCsv(table: Table, csv: SourceCSV, limit: Option[Int] = None, fieldEnclosed: String = "") = {
        val csvFile: Source = Source.fromFile(csv.filename)
        val lines = csvFile.getLines()

        val colnum = table.columns.size
        val colNames = table.columns.map(x => x.name)
        // TODO: Bug - if pageSize is set to non divisible number there seems to be some data corruption
        val pageSize = Config.bulkLoad.vectorSize // num of records per batch
        val tableSizePct = limit match {
                case Some(x) => x / math.min(20, x)
                case None => table.columns.foldLeft(0)((a, b) => a + b.size) * csvFile.length / 20
            }

        var pctLoaded = 0
        val loaders = table.columns.map(x => x.getLoader)
        var descriptors = mutable.HashMap[String, EncoderDescriptor#BlockDescriptor[_]]()

        table.columns.foreach(x => {
            x.enc match {
                case enc: EncoderDescriptor => {
                    descriptors += (x.name -> enc.descriptor(x))
                }
                case _ => Unit
            }
        })

        var counter = 0
        var dataCounter = 0
        var continue = true

        def checkLimit = {
            continue = limit match {
                case Some(x) => if (counter > x - 1) false else true
                case None => true
            }
        }

        def initBatch(x: Int, y: Int) = {
            val data = new Array[Array[String]](x)
            for (i <- 0 until colnum) data(i) = new Array[String](y)
            data
        }

        var data = initBatch(colnum, pageSize)
        checkLimit
        while (continue && lines.hasNext) {
            val line = lines.next.split(csv.delim)

            if (dataCounter < pageSize) {
                for (i <- 0 until colnum)
                    data(i)(dataCounter) = line(i).
                            replaceFirst(fieldEnclosed, "").
                            reverse.
                            replaceFirst(fieldEnclosed, "").
                            reverse

                dataCounter += 1
            } else {
                for (i <- 0 until colnum) {
                    loaders(i).write(data(i).toVector)

                    descriptors.get(colNames(i)) match {
                        case Some(x) => x.add(data(i).toVector)
                        case None => None
                    }
                }

                // Reset batch and insert new record
                data = initBatch(colnum, pageSize)
                dataCounter = 0

                for (i <- 0 until colnum)
                    data(i)(dataCounter) = line(i)

                dataCounter += 1
            }

            counter += 1

            if (counter % tableSizePct == 0) {
                pctLoaded += 5
                info(s"$pctLoaded% -- $counter records.")
            }

            checkLimit
        }

        if (dataCounter > 0 )
            for (i <- 0 until colnum) {
                loaders(i).write(data(i).slice(0, dataCounter).toVector)

                descriptors.get(colNames(i)) match {
                    case Some(x) => x.add(data(i).slice(0, dataCounter).toVector)
                    case None => None
                }
            }

        for (loader <- loaders) loader.close // finalize

        for (descr <- descriptors) descr._2.write

        val sizeFile = new FileWriter(new File(s"${Config.home}/${table.name}/_size"))
        sizeFile.write(counter.toString)
        sizeFile.close

        info(s"Total records loaded: $counter.")
    }
}

object DataLoaderMain extends App {
    def small = {
        val table = Table.loadTable("correla_dataset_small_new")
        val filename = "/Users/marcin/correla/correla_dataset_small.csv"
        val csv = SourceCSV(filename, '|', skipRows = 0)

        DataLoader.fromCsv(table, csv, Some(1000000))
    }

    def big = {
        val table = Table.loadTable("immutable2_100mil")
        val filename = "/Users/marcin/correla/correla_dataset_100mil.csv"
        val csv = SourceCSV(filename, '|', skipRows = 0)

        DataLoader.fromCsv(table, csv, Some(20000000))
    }

    def ame = {
        val table = Table.loadTable("ame_sample")
        val filename = "/Users/marcin/correla/ame_sample_immutable.psv"
        val csv = SourceCSV(filename, '|', skipRows = 0)

        DataLoader.fromCsv(table, csv, Some(20000000), "\"")
    }

    ame
}