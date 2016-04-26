package immutable

/**
  * Created by marcin on 2/21/16.
  */

object Stats {
    def histogram(vec: Vector[String], col: NumericColumn, size: Int): (Array[Double], Array[Int]) = {
        val bins = Array.fill(size)(0.0)
        val sortedVec = vec.map(x => col.num.toDouble(col.stringToValue(x))).toSet.toList.sorted
        val sep = sortedVec.size.toDouble / (size - 2)
        val binCount = Array.fill(size)(0)

        var nextSep: Int = 0

        bins(0) = Double.NegativeInfinity
        bins(size - 1) = Double.PositiveInfinity

        var counter: Int = 0
        while (counter < size - 2) {
            nextSep = (counter * sep).toInt
            bins(counter + 1) = sortedVec(nextSep)
            binCount(counter + 1) += 1
            counter += 1
        }
        (bins, binCount)
    }
}
