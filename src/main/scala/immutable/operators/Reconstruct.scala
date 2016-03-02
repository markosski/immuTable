package immutable.operators

import java.nio.ByteBuffer

import immutable.Table
import immutable.encoders.Encoder
import immutable.{VarCharColumn, FixedCharColumn}

/**
  * Created by marcin on 2/27/16.
  */
//object Reconstruct extends Operator {
//    def apply(colName: String, oidBuffer: ByteBuffer)(implicit table: Table): ByteBuffer = {
//        val col = table.getColumn(colName) match {
//            case x: FixedCharColumn => x.asInstanceOf[FixedCharColumn]
//            case x: VarCharColumn => x.asInstanceOf[VarCharColumn]
//        }
//        var iter = Encoder.getColumnIterator(col, table)
//
//
//    }
//}
