package immutable

/**
  * Created by marcin on 2/21/16.
  */
abstract class ColumnStats[A](sampleSize: Int, cardinality: Int)
case class StringColumnStats[A](sampleSize: Int, cardinality: Int) extends ColumnStats[A](sampleSize, cardinality)
case class NumericColumnStats[A](cardinality: Int, sampleSize: Int, min: A, max: A) extends ColumnStats[A](sampleSize, cardinality)
