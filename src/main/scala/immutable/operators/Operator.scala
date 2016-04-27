package immutable.operators

import java.nio.IntBuffer

import immutable.LoggerHelper._
import immutable.encoders.{Dict, Dense}
import immutable.{Config, Table}
import immutable._

import scala.util.{Try, Failure, Success}

/**
  * Created by marcin on 2/26/16.
  *
  */
trait SelectionOperator extends Iterable[Int] {
    def iterator: Iterator[Int]
}

trait ProjectionOperator extends Iterable[Seq[_]] {
    def iterator: Iterator[Seq[_]]
}

object SelectionOperator {
    // TODO: This functionality should be somewhere in Encoder.
    def prepareBuffer(col: Column, table: Table): Unit = {
        val ext = col match {
            case x: VarCharColumn => x.enc match {
                case Dict => "dict"
                case _ => throw new Exception("Codec not available for this column type.")
            }
            case x: FixedCharColumn => x.enc match {
                case Dense => "dense"
                case Dict => "dict"
                case _ => throw new Exception("Codec not available for this column type.")
            }
            case x: NumericColumn => x.enc match {
                case Dense => "dense"
                case Dict => "dict"
                case _ => throw new Exception("Codec not available for this column type.")
            }
        }

        Try(BufferManager.get(col.FQN)) match {
            case Success(x) => info(s"Buffer ${col.FQN} already registered.")
            case Failure(e) => {
                info(s"Registering new buffer: ${col.FQN}")
                BufferManager.registerFromFile(col.FQN, s"${Config.home}/${table.name}/${col.name}.${ext}", col.size)
            }
        }
    }
}

