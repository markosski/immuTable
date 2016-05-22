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
    def test1 = {
        val table = SchemaManager.getTable("correla_dataset_small_new")
        val res = Scan(table.column("fname").asInstanceOf[VarCharColumn]).iterator

        while (res.hasNext) {
            val vec = res.next
            println(vec.data(0)(0))
        }
    }

    def small = {
//        val table = SchemaManager.getTable("immutable2_100mil")
        val table = SchemaManager.getTable("correla_dataset_small_new")

        info("start --")

//        val res = Scan(table.column("fname").asInstanceOf[VarCharColumn])
//        val res1 = FetchSelectMatch(table.column("fname").asInstanceOf[VarCharColumn], List("Bristol", "Cephus", "Jennie"), res)
//        val res2 = FetchSelectRange(table.column("age").asInstanceOf[TinyIntColumn], "30", "100", res1)
//        val res3 = FetchSelectMatch(table.column("state").asInstanceOf[FixedCharColumn], List("CT", "NY", "NJ", "OH"), res2)

        val res1 = ScanSelectMatch(table.column("fname").asInstanceOf[VarCharColumn], List("Bristol", "Cephus", "Jennie"))
        val res2 = FetchSelectRange(table.column("age").asInstanceOf[TinyIntColumn], "30", "100", res1)
        val res3 = Fetch(table.column("state").asInstanceOf[FixedCharColumn], res2)

//        val res = ScanSelectRange(table.column("age").asInstanceOf[TinyIntColumn], "35", "70")
//        val res1 = FetchSelectRange(table.column("score1").asInstanceOf[TinyIntColumn], "75", "100", res)
//        val res2 = FetchSelectRange(table.column("score2").asInstanceOf[TinyIntColumn], "75", "100", res1)
//        val res3 = FetchSelectMatch(table.column("state").asInstanceOf[FixedCharColumn], List("CT", "NY", "NJ", "OH"), res2)

//        val result = Project(List(
//            table.column("fname").asInstanceOf[VarCharColumn],
//            table.column("state").asInstanceOf[FixedCharColumn],
//            table.column("age").asInstanceOf[TinyIntColumn],
//            table.column("score1").asInstanceOf[TinyIntColumn]
//        ), res4)
//
        val result = Aggregate(
            List(
                Count(table.column("age").asInstanceOf[TinyIntColumn]),
                Min(table.column("age").asInstanceOf[TinyIntColumn]),
                Max(table.column("age").asInstanceOf[TinyIntColumn])
        ), res3)

//        val result = ProjectAggregate(
//            List(table.column("state").asInstanceOf[FixedCharColumn]),
//            List(
//                Count(table.column("age").asInstanceOf[TinyIntColumn]),
//                Min(table.column("age").asInstanceOf[TinyIntColumn]),
//                Max(table.column("age").asInstanceOf[TinyIntColumn])
//            ), res3, Some(table.column("state").asInstanceOf[FixedCharColumn]))

//        result.take(10).foreach(x => println(x))

        result.foreach(x => println(x))
        info("end --")

    }

    def big = {
        val table = SchemaManager.getTable("immutable2_100mil")
        info("start --")

        val res = ScanSelectMatch(table.column("fname").asInstanceOf[VarCharColumn], List("Alexa"))
        val res1 = FetchSelectRange(table.column("age").asInstanceOf[TinyIntColumn], "20", "65", res)
        val res2 = FetchSelectMatch(table.column("state").asInstanceOf[FixedCharColumn], List("CT", "NY", "NJ", "TX"), res1)

//        val result = Project(List(
//            table.column("age").asInstanceOf[TinyIntColumn],
//            table.column("fname").asInstanceOf[VarCharColumn]
//        ), res2)

        val result = ProjectAggregate(
            List(table.column("age").asInstanceOf[TinyIntColumn]),
            List(
                Count(table.column("age").asInstanceOf[TinyIntColumn]),
                Avg(table.column("age").asInstanceOf[TinyIntColumn])
            ), res2, Some(table.column("state").asInstanceOf[FixedCharColumn]))
//
//        result.take(10).foreach(x => println(x))
        result.foreach(x => println(x))
        info("end --")
    }

    def ame = {
        val table = SchemaManager.getTable("ame_sample")
        info("start --")

        val res = ScanSelectMatch(table.column("zip").asInstanceOf[FixedCharColumn], List("22310", "11222"))
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
            case "test1" => { test1; chooseOption }
            case "ame" => { ame; chooseOption }
            case "exit" => { sys.exit() }
            case _ => println("unknown command"); chooseOption
        }
    }
    chooseOption
//    small
}
