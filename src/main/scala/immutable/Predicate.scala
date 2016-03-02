package immutable

sealed trait Predicate[A]

/**
  * Predicate for range match of NumericColumn type
  * @param col
  * @param min
  * @param max
  */
case class Range[A](col: Column[A] with NumericColumn, min: String, max: String) extends Predicate[A] {
    override def toString: String = s"range__${col.name}__${min}__${max}"
}

/**
  * Predicate for exact match; accepts list of values.
  * @param col
  * @param value
  */
case class Exact[A](col: Column[A], value: Seq[String]) extends Predicate[A] {
    override def toString: String = s"exact__${col.name}__${value}"
}

/**
  * Predicate of contains of CharColumn type.
  * @param col
  * @param value value to test
  * @param mode -1 = left, 0 = two ways, 1 = right
  */
case class Contains[A](col: Column[A] with CharColumn, value: String, mode: Int = 0) extends Predicate[A] {
    override def toString: String = s"contains__${col.name}__${value}"
}

