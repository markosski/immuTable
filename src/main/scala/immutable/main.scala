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

//        BufferManager.registerFromFile("age_0", s"${Config.home}/${table.name}/age_0.inter", 4)
//        BufferManager.registerFromFile("state_0", s"${Config.home}/${table.name}/state_0.inter", 4)
//        BufferManager.registerFromFile("score1_0", s"${Config.home}/${table.name}/score1_0.inter", 4, parts=Some(1))
//        BufferManager.registerFromFile("score2_0", s"${Config.home}/${table.name}/score2_0.inter", 4, parts=Some(1))

        info("start --")

        val res = Select(Exact(table.getColumn[VarCharColumn]("fname"), List("Jennie")), false)
        val res1 = FetchSelect(Exact(table.getColumn[FixedCharColumn]("state"), List("CT", "NY", "NJ")), res, false)
        val res2 = FetchSelect(Range(table.getColumn[TinyIntColumn]("score2"), "18", "100"), res1, false)
        val res3 = FetchSelect(Range(table.getColumn[TinyIntColumn]("score3"), "75", "100"), res2, false)
        val res4 = FetchSelect(Range(table.getColumn[TinyIntColumn]("age"), "18", "70"), res3, false)

//        val inter = Select(table.getColumn[FixedCharColumn]("state"), List("CT", "NY", "NJ"))
//        val res2 = Union(res, res1)
//        val res2 = FetchSelect(table.getColumn[TinyIntColumn]("score1"), res1, "50", "100")
//
//        Intermediate(table.getColumn[TinyIntColumn]("age"), table).encode(res)
//        Intermediate(table.getColumn[FixedCharColumn]("state"), table).encode(inter)

//        info("result: " + res4.limit/4)
//        val result = Project(List(
//            table.getColumn[VarCharColumn]("fname"),
//            table.getColumn[FixedCharColumn]("state"),
//            table.getColumn[FixedCharColumn]("zip"),
//            table.getColumn[TinyIntColumn]("score1")
//        ), res4).iterator
        for (i <- 0 until math.min(res4.limit/4, 10)) {
            println(res4.getInt)
        }

//        for (i <- 0 until 10) {
//            println(result.next)
//        }
        info("end --")

    }

    small
}
