package immutable

import immutable.encoders.{RunLength, Dense}
import scala.io.Source

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
                case 'Dense => Dense(col, table).encode(counter, csv)
                case 'RunLength => RunLength(col, table).encode(counter, csv)
                case _ => throw new Exception(s"Unknown encoder ${col.encoder}.")
            }
            counter += 1
        }
    }
}

object DataLoaderMain extends App {
    def small = {
        val table = Table.loadTable("correla_dataset_small")
        val filename = "/Users/marcin/correla/correla_dataset_small.csv"
        val csv = SourceCSV(filename, '|', skipRows = 0)

        DataLoader.fromCsv(table, csv)
    }

    def big = {
        val filename = "/Users/marcin/correla/correla_dataset_100mil.csv"
        val csv = SourceCSV(filename, '|', skipRows = 0)

        val table = Table(
            "immutable2_100mil",
            TinyIntColumn("age", encoder='RunLength),
            VarCharColumn("fname", 15),
            VarCharColumn("city", 30),
            FixedCharColumn("state", 2, encoder='RunLength),
            FixedCharColumn("zip", 5)
        )

        DataLoader.fromCsv(table, csv)
    }

    big
}
