
package immutable.imprints

import java.io.{BufferedInputStream, BufferedOutputStream, RandomAccessFile}
import java.nio.channels.FileChannel
import java.util.Date

import immutable.helpers._
import math.random
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.MappedByteBuffer
import scala.collection.mutable

/**
 * Created by marcin on 9/19/15.
 */
object ImprintHelpers {
    def histogram(data: Seq[Double], size: Int) = {
        val bins = Array.fill(size)(0.0)
        val sortedData = data.toSet.toList.sorted
        val sep = sortedData.size.toDouble / (size - 2)

        var counter: Int = 0
        var nextSep: Int = 0

        bins(0) = Double.NegativeInfinity
        bins(size - 1) = Double.PositiveInfinity
        while (counter < 62) {
            nextSep = (counter * sep).toInt
            bins(counter + 1) = sortedData(nextSep)
            counter += 1
        }
        bins
    }
}

case class ImprintCacheDict(counter: Int, repeat: Boolean, flags: Byte)

object ImprintIndex {
    /**
     * To make it more agnostic this function should accept sample from the outside
     * or screw it and pass in whole engine object :)
     */

    def mask(hist: Array[Double], low: Double, high: Double) = {
        var mask: Long = 0
        var min = 0
        var max = 0

        for (i <- 0 to hist.size - 1) {
            if (low >= hist(i)) min = i
            if (high >= hist(i)) max = i
        }

        for ( i <- min to max) mask |= 1.toLong << i
        mask
    }

    def innerMask(hist: Array[Double], low: Double, high: Double) = {
        var mask: Long = 0
        var min = 0
        var max = 0

        for (i <- 0 to hist.size - 1) {
            if (low > hist(i)) min = i
            if (high > hist(i)) {
                max = i
            } else if (max < min) {
                max = min
            }
        }

        for ( i <- min to max) mask |= 1.toLong << i
        mask
    }

    def buildHistogram(tableName: String, columnName: String) = {
        val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "rw")

        var corfBytes = new Array[Byte](4)
        var dataBytes = new Array[Byte](1)
        val size = (colFile.length() / 1).toInt
        val sampleSize = 2048

        val randIdx = for (i <- 0.toLong to sampleSize - 1) yield (math.random * size).toInt
        val sample = mutable.MutableList.fill(sampleSize)(0.0)

        var counter = 0
        randIdx foreach (idx => {
            colFile.seek(idx)
            colFile.read(dataBytes)
            sample(counter) = Conversions.bytesToInt(dataBytes).toDouble
            counter += 1
        })

        colFile.close()

        val hist = ImprintHelpers.histogram(sample, 64)
        hist
    }

    def create(tableName: String, columnName: String, hist: Array[Double]) = {
        val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r")
        val imprintFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.imp", "rw")

        var colBytes = new Array[Byte](4)
        var dataBytes = new Array[Byte](1)

        val size = (colFile.length() / 1).toInt
        val cacheLineSize = 64
        val cacheLineLength = 64 / 1  // Depending on size of the value
        val cacheLineIndex = Array.fill(size/cacheLineLength)(0.toLong)
        var cacheLineData = Array.fill(cacheLineLength)(0.toDouble)

        var counter = 0
        while (counter < size) {
            colFile.read(dataBytes)

            if (counter == 0 || counter % cacheLineLength != 0) {
                cacheLineData(counter % cacheLineLength) = Conversions.bytesToInt(dataBytes).toDouble
                counter += 1
            } else {
                cacheLineData foreach (item => {
                    for (i <- 0 to hist.size - 1) {
                        if (counter < size && item >= hist(i) && item < hist(math.min(i + 1, hist.size - 1))) {
                            cacheLineIndex((counter / cacheLineLength) - 1) |= (1.toLong << i)

                            if (counter > cacheLineLength && cacheLineIndex((counter / cacheLineLength) - 1) == cacheLineIndex((counter / cacheLineLength) - 2)) {
                                println("Match")
                            }
                        }
                    }
                })

                cacheLineData = Array.fill(cacheLineLength)(0.toDouble)
                cacheLineData(0) = Conversions.bytesToInt(dataBytes).toDouble
                counter += 1
            }
        }
        colFile.close()

        cacheLineIndex foreach (item => {
            imprintFile.writeLong(item)
        })
        imprintFile.close()

        cacheLineData
    }

    /**
     * When column enthropy is too high, consider doing full scan.
     * @param tableName
     * @param columnName
     * @param low
     * @param high
     * @param hist
     * @return
     */
    def read(tableName: String, columnName: String, low: Double, high: Double, hist: Array[Double]) = {
        val imprintFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.imp", "r")
        val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r")
        val colFileMapped = colFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, colFile.length)
        val buffer = ByteBuffer.allocateDirect(1000000 * 4).order(ByteOrder.nativeOrder())

        val size = imprintFile.length / 8
        val mask = ImprintIndex.mask(hist, low, high)
        val innerMask = ImprintIndex.innerMask(hist, low, high)
        val imprint = Array.fill(size.toInt)(0.toLong)
        val pageSize = 4096

        var counter = 0
        while (counter < imprint.size) {
            imprint(counter) = imprintFile.readLong()
            counter += 1
        }

        // TODO: add function that given oid returns value, maybe implemented in column?
        val valueSize = 1
        val dataBytes = new Array[Byte](valueSize)  // attribute value
        val pageBytes = new Array[Byte](pageSize)
        var dataValue = 0
        val cacheSize = 64/valueSize

        val lowInt = low.toInt
        val highInt = high.toInt

        val start = new Date().getTime()
        var impCounter = 0; var comparisons = 0
        while (impCounter < imprint.size) {
            if ((imprint(impCounter) & mask) > 0) {
                if ((imprint(impCounter) & ~innerMask) == 0) {
                    var i = 0
                    while (i < cacheSize) {
                        buffer.putInt(impCounter + i)
                        i += 1
                    }
                } else {
                    var j = 0
                    while (j < cacheSize) {
                        colFileMapped.get(dataBytes)
                        dataValue = Conversions.bytesToInt(dataBytes)
                        if (dataValue <= highInt && dataValue >= lowInt)
                            buffer.putInt(impCounter + j)
                        j += 1
                        comparisons += 1
                    }

                }
            }
            impCounter += 1
        }
        println("Finished scanning column imprint: " + (new Date().getTime() - start).toString + "ms")
        println("Size: " + buffer.position / 4)
        buffer.clear()
    }

    def testReadMappedFile(tableName: String, columnName: String) = {
        val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r")
//        val colFileMapped = colFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, colFile.length)

        val pageSize = 4096
        val colBytes = new Array[Byte](4)
        val pageBytes = new Array[Byte](pageSize)
        val buffer = ByteBuffer.allocate(colFile.length.toInt + 8)
        var counter = 0
        val start = new Date().getTime()
        while (counter < 1000000 / pageSize) {
            colFile.read(pageBytes)
            buffer.put(pageBytes)
            buffer.flip()
            while (buffer.position < pageSize) buffer.getInt
            buffer.clear()
            counter += 1
        }

        println("done: " + (new Date().getTime() - start).toString + "ms")
        buffer.clear()
        colFile.close()
    }
}

case class InvertedIndex() {

}

object ImprintTest extends App {
    val hist = ImprintIndex.buildHistogram("test_table_small", "age")
    ImprintIndex.create("test_table_small", "age", hist)
    ImprintIndex.read("test_table_small", "age", 30, 45, hist)
//    ImprintIndex.testReadMappedFile("test_table_small", "age")
}