package immutable

sealed trait Predicate[A] {
    val col: Column[A]
}

/**
  * Predicate for range match of NumericColumn type
  * @param col
  * @param min
  * @param max
  */
case class Range[A](col: Column[A] with NumericColumn[A], min: String, max: String) extends Predicate[A] {
    override def toString: String = s"range__${col.name},${min},${max}"
}

/**
  * Predicate for exact match; accepts list of values.
  * @param col
  * @param value
  */
case class Exact[A](col: Column[A], value: Seq[String]) extends Predicate[A] {
    override def toString: String = s"exact__${col.name},${value}"
}

/**
  * Predicate of contains of CharColumn type.
  * @param col
  * @param value value to test
  * @param mode -1 = left, 0 = two ways, 1 = right
  */
case class Contains[A](col: Column[A] with CharColumn, value: String, mode: Int = 0) extends Predicate[A] {
    override def toString: String = s"contains__${col.name},${value}"
}

/**
  * Execute function against column value
  * @param col
  * @param func
  * @tparam A
  */
case class Func[A](col: Column[A], func: (A) => Boolean) extends Predicate[A] {
    override def toString: String = s"func__${func}"
}
