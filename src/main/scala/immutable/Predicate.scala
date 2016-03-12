package immutable

sealed trait Predicate {
    val col: Column
}

/**
  * Predicate for range match of NumericColumn type
  * @param col
  * @param min
  * @param max
  */
case class Range(col: Column with NumericColumn, min: String, max: String) extends Predicate {
    override def toString: String = s"range__${col.name},${min},${max}"
}

/**
  * Predicate for exact match; accepts list of values.
  * @param col
  * @param value
  */
case class Exact(col: Column, value: Seq[String]) extends Predicate {
    override def toString: String = s"exact__${col.name},${value}"
}

/**
  * Predicate of contains of CharColumn type.
  * @param col
  * @param value value to test
  * @param mode -1 = left, 0 = two ways, 1 = right
  */
case class Contains(col: Column with CharColumn, value: String, mode: Int = 0) extends Predicate {
    override def toString: String = s"contains__${col.name},${value}"
}
