package immutable

import java.nio.ByteBuffer
import immutable.encoders.Intermediate
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

//        BufferManager.registerFromFile("age_0", s"${Config.home}/${table.name}/age_0.inter", 4)
//        BufferManager.registerFromFile("state_0", s"${Config.home}/${table.name}/state_0.inter", 4)
//        BufferManager.registerFromFile("score1_0", s"${Config.home}/${table.name}/score1_0.inter", 4, parts=Some(1))
//        BufferManager.registerFromFile("score2_0", s"${Config.home}/${table.name}/score2_0.inter", 4, parts=Some(1))

        info("start --")

//        val res = Select(Filter(table.getColumn[TinyIntColumn]("age"), (x: Byte) => { if (x == "55".toByte) true else false }), false)
        val res = Select(Exact(table.getColumn[VarCharColumn]("fname"), List("Jennie", "Melissa", "Bristol", "Cephus")), false)
//        val res1 = Select(Exact(table.getColumn[FixedCharColumn]("state"), List("CT", "NY", "NJ")), false)
//        val res2 = FetchSelect(Range(table.getColumn[TinyIntColumn]("score2"), "18", "100"), res1, false)
//        val res3 = FetchSelect(Range(table.getColumn[TinyIntColumn]("score3"), "75", "100"), res2, false)
        val res4 = FetchSelect(Range(table.getColumn[TinyIntColumn]("age"), "18", "70"), res, false)

//        val inter = Select(table.getColumn[FixedCharColumn]("state"), List("CT", "NY", "NJ"))
//        val res2 = Union(res, res1)
//        val res2 = FetchSelect(table.getColumn[TinyIntColumn]("score1"), res1, "50", "100")
//
//        Intermediate(table.getColumn[TinyIntColumn]("age"), table).encode(res)
//        Intermediate(table.getColumn[FixedCharColumn]("state"), table).encode(inter)

        info("result: " + res4.limit/4)
        val result = Project(List(
            table.getColumn[VarCharColumn]("fname"),
            table.getColumn[FixedCharColumn]("state"),
            table.getColumn[FixedCharColumn]("zip"),
            table.getColumn[TinyIntColumn]("score1")
        ), res4).iterator
//        for (i <- 0 until math.min(res4.limit/4, 10)) {
//            println(res2.getInt)
//        }

        for (i <- 0 until 10) {
            println(result.next)
        }
        info("end --")

    }

    def big = {
        implicit val table = Table.loadTable("immutable2_100mil")

        BufferManager.registerFromFile("fname_0", s"${Config.home}/${table.name}/fname_0.inter", 4)
        BufferManager.registerFromFile("state_0", s"${Config.home}/${table.name}/state_0.inter", 4)

        info("-- start --")
//        val res = Select(table.getColumn[VarCharColumn]("fname"), "0", true)
//        var res1 = FetchSelect(table.getColumn[FixedCharColumn]("state"), res, List("CT", "NY", "NJ"), true)

//        val inter = Select(table.getColumn[VarCharColumn]("fname"), "0", true)
//        Intermediate(table.getColumn[FixedCharColumn]("state"), table).encode(inter)
//        Intermediate(table.getColumn[VarCharColumn]("fname"), table).encode(inter)
//        val res10 = table.columns(1) match {
//            case x: TinyIntColumn => Select(x, "0", "20")
//            case x: VarCharColumn => Select(x, "0")
//            case _ => throw new Exception("Unknown column type")
//        }

        info("-- end --")
//        println(res1.limit/4)
    }

    small
}
