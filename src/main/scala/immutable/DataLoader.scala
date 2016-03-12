package immutable

import immutable.encoders._
import scala.io.Source
import immutable.LoggerHelper._

/**
  * Created by marcin on 2/9/16.
  */

case class SourceCSV(filename: String, delim: Char, skipHeader: Boolean = false, enclosed: Option[Char] = None, skipRows: Int = 0)

object DataLoader {
    /**
      *
      * @param table - Relation/Table
      * @param csv - SourceCSV object
      */
    def fromCsv(table: Table, csv: SourceCSV) = {
        val csvFile: Source = Source.fromFile(csv.filename)
        val lines = csvFile.getLines()

        if (csv.skipHeader) lines.next()

        val firstLine = lines.next.split(csv.delim)

        if (firstLine.size != table.columns.size)
            throw new Exception("Number of columns in source file and table does not match")

        var counter = 0
        for (col <- table.columns) {
            col.encoder match {
                case 'Dense => Dense(col, table).loader
                case 'RunLength => RunLength(col, table).loader
                case _ => throw new Exception(s"Unknown encoder ${col.encoder}.")
            }
            counter += 1
        }
    }
}

object DataLoaderNew {
    def fromCsv(table: Table, csv: SourceCSV) = {
        val csvFile: Source = Source.fromFile(csv.filename)
        val lines = csvFile.getLines()

        val colnum = table.columns.size
        val data = new Array[Array[String]](colnum)
        val pageSize = 4096
        val tableSizePct = table.size / 20 // report every 5%
        var pctLoaded = 0
        for (i <- 0 until colnum) data(i) = new Array[String](pageSize)

        val loaders = new Array[Loader](table.columns.size)

        var i = 0
        for (col <- table.columns) {
            loaders(i) = col.encoder match {
                case 'Dict => Dict(col, table).loader
                case 'RunLength => RunLength(col, table).loader
                case 'Dense => Dense(col, table).loader
                case _ => throw new Exception(s"Unknown encoder ${col.encoder}.")
            }
            i += 1
        }

        var counter = 0
        var dataCounter = 0
        while (lines.hasNext) {
            val line = lines.next.split(csv.delim)

            if (dataCounter < pageSize) {
                for (i <- 0 until colnum) data(i)(dataCounter) = line(i)
                dataCounter += 1
            } else {
                for (i <- 0 until colnum) loaders(i).load(data(i).toVector)
                dataCounter = 0
                for (i <- 0 until colnum) data(i)(dataCounter) = line(0)
            }
            counter += 1

            if (counter % tableSizePct == 0) {
                pctLoaded += 5
                info(s"$pctLoaded% -- $counter records.")
            }
        }
        if (dataCounter > 0 ) for (i <- 0 until colnum) loaders(i).load(data(i).toVector)

        for (loader <- loaders) loader.finish // finalize
    }
}

object DataLoaderMain extends App {
    def small = {
        val table = Table.loadTable("correla_dataset_small")
        val filename = "/Users/marcin/correla/correla_dataset_small.csv"
        val csv = SourceCSV(filename, '|', skipRows = 0)

        DataLoader.fromCsv(table, csv)
    }

    def smallNew = {
        val table = Table.loadTable("correla_dataset_small_new")
        val filename = "/Users/marcin/correla/correla_dataset_small.csv"
        val csv = SourceCSV(filename, '|', skipRows = 0)

        DataLoaderNew.fromCsv(table, csv)
    }

    def big = {
        val table = Table.loadTable("immutable2_100mil")
        val filename = "/Users/marcin/correla/correla_dataset_100mil.csv"
        val csv = SourceCSV(filename, '|', skipRows = 0)

        DataLoaderNew.fromCsv(table, csv)
    }

    smallNew
}
