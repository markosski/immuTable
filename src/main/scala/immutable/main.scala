package immutable

import java.nio.IntBuffer

import immutable.encoders.Dict
import immutable.operators._
import immutable.LoggerHelper._
import immutable._
import java.util.Date

/**
  * Created by marcin on 2/21/16.
  */

object Main extends App {
    def small = {
        val table = SchemaManager.getTable("correla_dataset_small_new")

        info("start --")
//        val res1 = FetchSelectMatch(table.column[FixedCharColumn]("state"), List("CT", "NY", "NJ"), res)

        val res = SelectRange(table.column[TinyIntColumn]("age"), "25", "75")
        val res1 = FetchSelectMatch(table.column[VarCharColumn]("fname"), List("Marcin", "Cephus", "Jennie"), res)
        val res2 = FetchSelectMatch(table.column[FixedCharColumn]("state"), List("CT", "NY", "NJ", "VA"), res1)

//        Intermediate(table.getColumn[TinyIntColumn]("age"), table).encode(res)
//        Intermediate(table.getColumn[FixedCharColumn]("state"), table).encode(inter)

//        val resIter = res.iterator
//        resIter foreach(x => Unit)
//        println(s"skipped times... ${resIter.descriptorSkipped}")

        val result = Project(List(
            table.column[VarCharColumn]("fname"),
            table.column[FixedCharColumn]("state"),
            table.column[TinyIntColumn]("age")
        ), res2)

//        val result = ProjectAggregate(
//            List(),
//            List(
//                Min(table.column[TinyIntColumn]("age")),
//                Max(table.column[TinyIntColumn]("age")),
//                Avg(table.column[TinyIntColumn]("age"))
//        ), res2)

//        val result = ProjectAggregate(
//            List(table.column[VarCharColumn]("fname")),
//            List(
//                Count(table.column[TinyIntColumn]("age")),
//                Min(table.column[TinyIntColumn]("age")),
//                Max(table.column[TinyIntColumn]("age"))
//            ), res2, Some(table.column[VarCharColumn]("fname")))

//        result.take(10).foreach(x => println(x))

        result.foreach(x => x)

        info("end --")

    }

    def big = {
        val table = SchemaManager.getTable("immutable2_100mil")

        info("start --")
//        val res = SelectRange(table.column[TinyIntColumn]("age"), "90", "127")
//        val res1 = FetchSelectMatch(table.column[FixedCharColumn]("state"), List("CT", "NY", "NJ", "TX"), res)

        val res = SelectMatch(table.column[VarCharColumn]("fname"), List("Jennie"))
        val res1 = FetchSelectRange(table.column[TinyIntColumn]("age"), "18", "127", res)

//        val resIter = res1.iterator
//        resIter foreach(x => Unit)
//        info(s"skipped...${resIter.descriptorSkipped}")

//        val result = Project(List(
//            table.column[VarCharColumn]("fname"),
//            table.column[FixedCharColumn]("state"),
//            table.column[TinyIntColumn]("age")
//        ), res1)

        val result = ProjectAggregate(
            List(),
            List(
                Count(table.column[VarCharColumn]("fname"))
        ), res1)

//        val result = ProjectAggregate(
//            List(
////                table.column[VarCharColumn]("fname")
//            ),
//            List(
//                Count(table.column[TinyIntColumn]("age")),
//                Avg(table.column[TinyIntColumn]("age"))
//            ), res1, Some(table.column[VarCharColumn]("fname")))

        result.take(100).foreach(x => println(x))
        info("end --")
    }

    SchemaManager.register(Table.loadTable("correla_dataset_small_new"))
    SchemaManager.register(Table.loadTable("immutable2_100mil"))

    def chooseOption: Unit = {
        val input = scala.io.StdIn.readLine("Enter command, [run|exit]: ")

        input match {
            case "small" => { small; chooseOption }
            case "big" => { big; chooseOption }
            case "exit" => { sys.exit() }
            case _ => println("unknown command"); chooseOption
        }
    }
    chooseOption
//    small
}
