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

        val data = new DataVectorProducer(table, List(
            table.column("fname").asInstanceOf[VarCharColumn],
            table.column("age").asInstanceOf[TinyIntColumn],
            table.column("state").asInstanceOf[FixedCharColumn]
        ))

        val res = SelectMatch(table.column("fname").asInstanceOf[VarCharColumn], List("Bristol", "Cephus", "Jennie"), data)
        val res1 = FetchSelectRange(table.column("age").asInstanceOf[TinyIntColumn], "18", "25", res)
        val res2 = FetchSelectMatch(table.column("state").asInstanceOf[FixedCharColumn], List("CT", "NY", "NJ", "OH"), res1)

//        val colIter = table.column[TinyIntColumn]("age").getIterator
//        val dataVec = colIter.next
//        print(colIter.next)

        val iter = res2.iterator
//        val oids = iter.next
//        while (oids.hasRemaining) {
//            print(oids.get)
//            print(", ")
//        }
//        for (i <- 0 until 10) {
        while (iter.hasNext) {
            val vec = iter.next
            if (vec.selected.size > 0 ) {
                print("")
            }
        }
//        iter.next

//        val result = Project(List(), res1)



//        Intermediate(table.getColumn[TinyIntColumn]("age"), table).encode(res)
//        Intermediate(table.getColumn[FixedCharColumn]("state"), table).encode(inter)

//        val result = Project(List(
//            table.getColumn[VarCharColumn]("fname"),
//            table.getColumn[FixedCharColumn]("state"),
//            table.getColumn[TinyIntColumn]("age")
//        ), res1)
//
//        val result = ProjectAggregate(
//            List(),
//            List(
//                Count(table.column("age").asInstanceOf[TinyIntColumn]),
//                Min(table.column("age").asInstanceOf[TinyIntColumn]),
//                Max(table.column("age").asInstanceOf[TinyIntColumn])
//        ), res1)

//        val result = ProjectAggregate(
//            List(table.column[VarCharColumn]("fname")),
//            List(
//                Count(table.column[TinyIntColumn]("age")),
//                Min(table.column[TinyIntColumn]("age")),
//                Max(table.column[TinyIntColumn]("age"))
//            ), res1, Some(table.column[VarCharColumn]("fname")))

//        result.take(200).foreach(x => println(x))

        info("end --")

    }

    def big = {
//        val table = SchemaManager.getTable("immutable2_100mil")
//
//        info("start --")
//        val res = SelectMatch(table.column[FixedCharColumn]("state"), List("CT", "NY", "NJ", "TX"))
//        val res1 = FetchSelectRange(table.column[TinyIntColumn]("age"), "18", "55", res)
//        val res2 = FetchSelectMatch(table.column[FixedCharColumn]("state"), List("CT"), res1)

//        val result = Project(List(
//            table.column[VarCharColumn]("fname"),
//            table.column[TinyIntColumn]("age")
//        ), res1)

//        val result = ProjectAggregate(
//            List(table.column[VarCharColumn]("fname")),
//            List(
//                Count(table.column[TinyIntColumn]("age")),
//                Avg(table.column[TinyIntColumn]("age"))
//            ), res1)
//
//        result.take(1000).foreach(x => println(x))
        info("end --")
    }

    SchemaManager.register(Table.loadTable("correla_dataset_small_new"))
    SchemaManager.register(Table.loadTable("immutable2_100mil"))

    def chooseOption: Unit = {
        val input = scala.io.StdIn.readLine("Enter command, [run|exit]: ")

        input match{
            case "small" => { small; chooseOption }
            case "big" => { big; chooseOption }
            case "exit" => { sys.exit() }
            case _ => println("unknown command"); chooseOption}
    }
    chooseOption
//    small
}
