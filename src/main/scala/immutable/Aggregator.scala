package immutable

import scala.collection.mutable

/**
  * Created by marcin on 3/10/16.
  */
trait Aggregator {
    type T = col.DataType
    type RType
    val col: Column
    def add(value: T)
    def get: RType
    def reset: Unit
}

case class Max(col: NumericColumn) extends Aggregator {
    type RType = col.DataType
    var result = col.min

    def add(compare: col.DataType): Unit = {
        if (col.num.gt(compare, result)) result = compare
    }

    def get = result

    def reset = result = col.min
}

case class Min(col: NumericColumn) extends Aggregator {
    type RType = col.DataType
    private var result = col.max

    def add(compare: col.DataType): Unit = {
        if (col.num.lt(compare, result)) result = compare
    }

    def get = result

    def reset = result = col.max
}

case class Avg(col: NumericColumn) extends Aggregator {
    type RType = Double
    private var result = BigInt(0)
    private var counter = 0L

    def add(value: col.DataType): Unit = {
        result += col.num.toLong(value)
        counter += 1
    }

    def get = (BigDecimal(result) / BigDecimal(counter)).toDouble

    def reset = result = BigInt(0)
}

case class Count(col: Column) extends Aggregator {
    type RType = Long
    private var counter = 0L

    override def add(value: col.DataType): Unit = {
        counter += 1
    }

    def get = counter

    def reset = counter = 0L
}

// TODO: WIP
case class Distinct(col: Column) extends Aggregator {
    type RType = Long
    private var set = mutable.Set[col.DataType]()

    override def add(value: col.DataType): Unit = {
        set.add(value)
    }

    def get = set.size

    def reset = Unit
}
