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
//        val res = SelectRange(table.column[TinyIntColumn]("age"), "18", "19")
//        val res1 = FetchSelectMatch(table.column[FixedCharColumn]("state"), List("CT", "NY", "NJ"), res)

        val res = SelectRange(table.column[TinyIntColumn]("age"), "35", "75")
        val res1 = FetchSelectRange(table.column[TinyIntColumn]("score1"), "70", "100", res)
        val res2 = FetchSelectRange(table.column[TinyIntColumn]("score2"), "70", "100", res1)
        val res3 = FetchSelectMatch(table.column[VarCharColumn]("fname"), List("Marcin", "Cephus", "Jennie"), res2)

//        val res = SelectMatch(table.column[VarCharColumn]("fname"), List("Marcin", "Cephus", "Jennie"))
//        val res1 = FetchSelectMatch(table.column[FixedCharColumn]("state"), List("CT", "NY", "NJ", "VA"), res)
//        val res2 = FetchSelectRange(table.column[TinyIntColumn]("age"), "18", "70", res1)

//        Intermediate(table.getColumn[TinyIntColumn]("age"), table).encode(res)
//        Intermediate(table.getColumn[FixedCharColumn]("state"), table).encode(inter)

//        val resIter = res.iterator
//        resIter foreach(x => Unit)
//        println(s"skipped times... ${resIter.descriptorSkipped}")

//        val result = Project(List(
//            table.column[VarCharColumn]("fname"),
//            table.column[TinyIntColumn]("score1"),
////            table.column[TinyIntColumn]("score2"),
//            table.column[TinyIntColumn]("age")
//        ), res2)

        val result = ProjectAggregate(
            List(),
            List(
                Min(table.column[TinyIntColumn]("age")),
                Max(table.column[TinyIntColumn]("age")),
                Avg(table.column[TinyIntColumn]("age"))
        ), res3)

//        val result = ProjectAggregate(
//            List(table.column[FixedCharColumn]("state")),
//            List(
//                Count(table.column[TinyIntColumn]("age")),
//                Min(table.column[TinyIntColumn]("age")),
//                Max(table.column[TinyIntColumn]("age"))
//            ), res3, Some(table.column[VarCharColumn]("state")))

        result.foreach(x => println(x))

        info("end --")

    }

    def big = {
        val table = SchemaManager.getTable("immutable2_100mil")

        info("start --")
//        val res = SelectRange(table.column[TinyIntColumn]("age"), "90", "127")
//        val res1 = FetchSelectMatch(table.column[FixedCharColumn]("state"), List("CT", "NY", "NJ", "TX"), res)

        val res = SelectRange(table.column[TinyIntColumn]("age"), "18", "127")
        val res1 = FetchSelectMatch(table.column[VarCharColumn]("fname"), List("Jennie"), res)
        val res2 = FetchSelectMatch(table.column[FixedCharColumn]("state"), List("CT", "NY", "NJ", "TX"), res1)

//        val resIter = res1.iterator
//        resIter foreach(x => Unit)
//        info(s"skipped...${resIter.descriptorSkipped}")

//        val result = Project(List(
//            table.column[VarCharColumn]("fname"),
//            table.column[FixedCharColumn]("state"),
//            table.column[TinyIntColumn]("age")
//        ), res1)

//        val result = ProjectAggregate(
//            List(),
//            List(
//                Count(table.column[VarCharColumn]("fname"))
//        ), res1)

        val result = ProjectAggregate(
            List(
                table.column[FixedCharColumn]("state")
            ),
            List(
                Count(table.column[TinyIntColumn]("age")),
                Avg(table.column[TinyIntColumn]("age"))
            ), res2, Some(table.column[VarCharColumn]("state")))

        result.foreach(x => println(x))
        info("end --")
    }

    def ame = {
        val table = SchemaManager.getTable("ame_sample")
        info("start --")

        val res = SelectMatch(table.column("zip").asInstanceOf[FixedCharColumn], List("22310", "11222"))
        val res1 = FetchSelectRange(table.column("income").asInstanceOf[IntColumn], "0", "200000", res)
        val res2 = FetchSelectRange(table.column("age").asInstanceOf[TinyIntColumn], "25", "45", res1)

        //        val result = Project(List(
        //            table.column("age").asInstanceOf[TinyIntColumn],
        //            table.column("fname").asInstanceOf[VarCharColumn]
        //        ), res2)

        val result = ProjectAggregate(
            List(table.column("age").asInstanceOf[TinyIntColumn]),
            List(
                Count(table.column("age").asInstanceOf[TinyIntColumn]),
                Avg(table.column("age").asInstanceOf[TinyIntColumn])
            ), res2, Some(table.column("zip").asInstanceOf[FixedCharColumn]))
        //
        //        result.take(10).foreach(x => println(x))
        result.foreach(x => println(x))
        info("end --")
    }

    SchemaManager.register(Table.loadTable("correla_dataset_small_new"))
    SchemaManager.register(Table.loadTable("immutable2_100mil"))
    SchemaManager.register(Table.loadTable("ame_sample"))

    def chooseOption: Unit = {
        val input = scala.io.StdIn.readLine("Enter command, [run|exit]: ")

        input match {
            case "small" => { small; chooseOption }
            case "big" => { big; chooseOption }
            case "ame" => { ame; chooseOption }
            case "exit" => { sys.exit() }
            case _ => println("unknown command"); chooseOption
        }
    }
    chooseOption
//    small
}
