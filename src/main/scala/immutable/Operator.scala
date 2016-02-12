package immutable

import java.nio.ByteBuffer

import main.scala.immutable.Column

/**
  * Created by marcin on 2/10/16.
  *
  * val res0 = SelectScan(colA, 10, 20)
  * val res1 = SelectScanFetch(colB, res0, 0, 25)
  * val res2 = SelectScanFetch(colC, res1, "Marcin")
  *
  * SelectScan
  * SelectScanFetch
  * SelectIndex
  * Fetch
  * Intersect
  * Union
  */
trait Operator

object Select extends Operator {
    def apply[A](col: Column[A], min: A, max: A, ge: Boolean = true, le: Boolean = true): Unit = ???

    def apply[A](col: Column[A], exact: A): Unit = ???
}

object Fetch extends Operator {
    def apply[A](col: Column[A], oid: ByteBuffer): Unit = ???
}
