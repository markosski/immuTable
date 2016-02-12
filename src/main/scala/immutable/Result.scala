package main.scala.immutable
import java.nio.{ByteBuffer, MappedByteBuffer}
/**
 * Created by marcin on 9/9/15.
 *
 * Try parallel process each binary operation.
 *
 * a = Select(first_name, "Marcin")
 * b = SelectRange(age, 25, 35)
 * res = Intersect(a, b)
 *
 * a = Select(first_name, "Marcin")
 * b = Select(last_name, "Kossakowski")
 * c = Intersect(a, b)
 * res = fetch(first_name, last_name)(c)
 *
 */
case class Result(buffer: ByteBuffer, size: Int)
