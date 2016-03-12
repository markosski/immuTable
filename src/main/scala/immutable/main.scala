package immutable

import java.nio.ByteBuffer
import immutable.helpers.{Conversions, Timer}
import immutable.operators._
import immutable.LoggerHelper._
import immutable._
import java.util.Date
/**
  * Created by marcin on 2/21/16.
  */
object Main extends App {
    def small = {
        implicit val table = Table.loadTable("correla_dataset_small_new")

        info("start --")
        val res = Select(Exact(table.getColumn[VarCharColumn]("fname"), List("Jennie", "Bristol", "Cephus")), false)
        val res1 = FetchSelect(Exact(table.getColumn[FixedCharColumn]("state"), List("CT", "NY", "NJ")), res, false)
        val res2 = FetchSelect(Range(table.getColumn[TinyIntColumn]("score2"), "18", "100"), res1, false)
        val res3 = FetchSelect(Range(table.getColumn[TinyIntColumn]("score3"), "18", "100"), res2, false)
        val res4 = FetchSelect(Range(table.getColumn[TinyIntColumn]("age"), "18", "70"), res3, false)

//        Intermediate(table.getColumn[TinyIntColumn]("age"), table).encode(res)
//        Intermediate(table.getColumn[FixedCharColumn]("state"), table).encode(inter)

        val result = Project(List(
            table.getColumn[VarCharColumn]("fname"),
            table.getColumn[FixedCharColumn]("state"),
            table.getColumn[TinyIntColumn]("age")
        ), res4)

        result.foreach(x => println(x))

        info("end --")

    }

    def chooseOption: Unit = {
        val input = scala.io.StdIn.readLine("Enter command, [run|exit]: ")

        input match{
            case "run" => { small; chooseOption }
            case "exit" => { sys.exit() }
            case _ => println("unknown command")}
    }
    chooseOption
}
