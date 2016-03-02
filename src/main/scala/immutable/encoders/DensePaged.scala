package immutable.encoders

import java.io.{FileOutputStream, BufferedOutputStream}
import java.nio.ByteBuffer

import immutable.LoggerHelper._
import immutable.{Config, SourceCSV, Table, Column, NumericColumn}

import scala.io.Source

/**
  * Created by marcin on 3/1/16.
  *
  */
//case class DensePaged[A](col: Column[A] with NumericColumn, table: Table) extends Iterable[(Int, A)] {
//    val pageSize = 1024
//    val minSize = 4
//    val maxSize = 4
//
//    private def encode(data: Vector[String]) = {
//        val colFile = new BufferedOutputStream(
//                new FileOutputStream(s"${Config.home}/${table.name}/${col.name}.dat", false),
//                Config.readBufferSize)
//
//        var i = 0
//        while (i <= data.size) {
//            val field_val = col.stringToBytes(data(i))
//            colFile.write(field_val)
//            i += 1
//        }
//        colFile.close()
//    }
//}
