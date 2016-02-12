package immutable

import main.scala.immutable._

import scala.io.Source

/**
  * Created by marcin on 2/9/16.
  */

case class SourceCSV(filename: String, delim: Char, skipHeader: Boolean = false, enclosed: Option[Char] = None, skipRows: Int = 0)

object DataLoader {
    /**
      *
      * @param name - Relation name
      * @param csv - SourceCSV object
      * @param cols - sequence of Column to represent data source
      */
    def fromCsv(name: String, csv: SourceCSV, cols: Column[_]*) = {
        val csvFile: Source = Source.fromFile(csv.filename)
        val lines = csvFile.getLines()

        if (csv.skipHeader) lines.next()

        val firstLine = lines.next.split(csv.delim)

        if (firstLine.size != cols.size)
            throw new Exception("Number of columns in source file and data definition does not match")

        var counter = 0
        for (col <- cols) {
            Dense(col, name).encode(counter, csv)
            counter += 1
        }
    }
}

object DataLoaderMain extends App {
//    val filename = "/Users/marcin/correla/Salaries.csv"
//    val csv = SourceCSV(filename, ',', skipHeader = true, skipRows = 1)
//
//    DataLoader.fromCsv(
//        "baseball",
//        csv,
//        ShortIntColumn("year"),
//        FixedCharColumn("teamid", 3),
//        FixedCharColumn("lgid", 2),
//        VarCharColumn("player", 15),
//        IntColumn("salary")
//    )

    val filename = "/Users/marcin/correla/correla_dataset_small.csv"
    val csv = SourceCSV(filename, '|', skipRows = 0)

    DataLoader.fromCsv(
        "correla_dataset_small",
        csv,
        TinyIntColumn("age"),
        VarCharColumn("fname", 15),
        VarCharColumn("lname", 30),
        VarCharColumn("city", 30),
        FixedCharColumn("state", 2),
        FixedCharColumn("zip", 5),
        TinyIntColumn("score1"),
        TinyIntColumn("score2"),
        TinyIntColumn("score3"),
        TinyIntColumn("score4")
    )
}
