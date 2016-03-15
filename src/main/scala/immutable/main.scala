package immutable

import java.nio.ByteBuffer
import immutable.helpers.{Conversions, Timer}
import immutable.operators._
import immutable.LoggerHelper._
import immutable._
import java.util.Date

import scala.collection.mutable

/**
  * Created by marcin on 2/21/16.
  */

object Main extends App {
    def small = {
        val table = Table.loadTable("correla_dataset_small_new")
        SchemaManager.register(table)

        info("start --")
//        val res = SelectRange(table.getColumn[TinyIntColumn]("age"), "18", "90")
        val res = Select(table.getColumn[VarCharColumn]("fname"), List("Jennie", "Bristol", "Cephus"))
        val res1 = FetchSelectMatch(table.getColumn[FixedCharColumn]("state"), List("CT", "NY", "NJ"), res)
        val res2 = FetchSelectRange(table.getColumn[TinyIntColumn]("age"), "18", "35", res1)
//        val vec = res.iterator.next
//        while(vec.hasRemaining) println(vec.get)

//        val res2 = FetchSelect(Range(table.getColumn[TinyIntColumn]("score2"), "18", "100"), res1, false)
//        val res3 = FetchSelect(Range(table.getColumn[TinyIntColumn]("score3"), "18", "100"), res2, false)

//        Intermediate(table.getColumn[TinyIntColumn]("age"), table).encode(res)
//        Intermediate(table.getColumn[FixedCharColumn]("state"), table).encode(inter)

//        println(res.foldLeft(0)((a, b) => a + b.limit))

        val result = Project(List(
            table.getColumn[VarCharColumn]("fname"),
            table.getColumn[FixedCharColumn]("state"),
            table.getColumn[TinyIntColumn]("age")
        ), res2)
        result.take(100).foreach(x => println(x))

//        res2.foreach(x => {
//            while (x.hasRemaining) {
//                println(x.get)
//            }
//        })

        info("end --")

    }

    def big = {
        val table = Table.loadTable("immutable2_100mil")
        SchemaManager.register(table)

        info("start --")
        val res = Select(table.getColumn[VarCharColumn]("fname"), List("Jennie", "Bristol", "Cephus"))
        val res1 = FetchSelectRange(table.getColumn[TinyIntColumn]("age"), "18", "70", res)
        val res2 = FetchSelectMatch(table.getColumn[FixedCharColumn]("state"), List("CT"), res1)

        val result = Project(List(
            table.getColumn[VarCharColumn]("fname"),
            table.getColumn[FixedCharColumn]("state"),
            table.getColumn[TinyIntColumn]("age")
        ), res2)

        result.take(1000).foreach(x => println(x))
        info("end --")
    }

    def chooseOption: Unit = {
        val input = scala.io.StdIn.readLine("Enter command, [run|exit]: ")

        input match{
            case "small" => { small; chooseOption }
            case "big" => { big; chooseOption }
            case "exit" => { sys.exit() }
            case _ => println("unknown command")}
    }
    chooseOption
//    big
}
