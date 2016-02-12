package experiments

/**
 * Created by marcin on 8/19/15.
 *
 * https://gist.github.com/vetler/1480346
 */

import java.io.RandomAccessFile
import scala.io.Source
import java.io
import scala.collection.mutable.HashMap

object indexColumn {

    def bytesToInt(bytes: Array[Byte]): Int = bytes.reverse.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF))

    def intToBytes(i: Int): Array[Byte] = List(8, 16, 24).foldLeft(Array[Byte]( (i & 0xFF).toByte) )((b,a) => b ++ Array[Byte]( ((i >> a) & 0xFF).toByte ))

    def createIndex(file: Source, cols: List[Int], colSize: List[Int]): HashMap[String, List[Int]] = {
        val index = new HashMap[String, List[Int]]()

        var counter = 0
        for (line <- file.getLines()) {
            val parts = line.split("\\|")
//            val fname = parts(1).getBytes.padTo(colSize(0), 0.toByte)
            val field_val = parts(1)

            if (index.contains(field_val)) {
                index.put(field_val, index.getOrElse(field_val, List()) ++ List(counter))
            } else {
                index.put(field_val, List(counter))
            }
            counter += 1
        }
        println("Index Size: " + index.size)
        val indexOffsets = new RandomAccessFile("/tmp/correla/fname_offsets.index", "rw")
        val indexLookup = new RandomAccessFile("/tmp/correla/fname_lookup.index", "rw")

        var startOffset = 0
        var endOffset = 0
        index.foreach(item => {
            startOffset = endOffset
            endOffset = startOffset + (4 * item._2.length)
            indexOffsets.write(item._1.getBytes.padTo(colSize(0), 0.toByte))
            indexOffsets.write(intToBytes(startOffset))
            indexOffsets.write(intToBytes(endOffset))

            item._2 foreach(CID => indexLookup.write(intToBytes(CID)))
        })

        indexLookup.close()
        indexOffsets.close()
        return index
    }

    def readIndex(): HashMap[String, Array[Int]] = {
        println("Started reading index.")
        val indexOffsets = new RandomAccessFile("/tmp/correla/fname_offsets.index", "r")
        val size: Long = indexOffsets.length / 20

        val index = HashMap[String, Array[Int]]()

        for (i <- 0.toLong to size) {
            val data = new Array[Byte](20)
            indexOffsets.read(data)
            index.put(new String(data.slice(0, 12).filter(_ != 0)), Array(bytesToInt(data.slice(12, 16)), bytesToInt(data.slice(16,20))))
        }
        println("Finished reading index.")
        index
    }

    def searchCID(index: HashMap[String, Array[Int]], value: String): List[Int] = {
        val offsets = index.getOrElse(value, Array[Int]())
        var result = List[Int]()

        if (offsets.size == 2) {
            val indexLookup = new RandomAccessFile("/tmp/correla/fname_lookup.index", "r")
            indexLookup.seek(offsets(0))

            for (i <- 1 to (offsets(1) - offsets(0)) / 4) {
                val data = new Array[Byte](4)
                indexLookup.read(data)
                result = result ++ List(bytesToInt(data))
            }
            indexLookup.close()
        }
        result
    }

//    def searchIndex(): HashMap[String, List[Int]] = {
//        val in = new io.FileInputStream("/tmp/data_binary_zip.index")
//        val bytes = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
//        val idx: HashMap[String, List[Int]] = Marshal.load[HashMap[String, List[Int]]](bytes)
//        idx
//    }
}

//object Main extends App {
//
////    val dataset: Source = Source.fromFile("/tmp/correla_dataset_small.csv")
////    val preIndex = indexColumn.createIndex(dataset, List(1), List(12, 1, 2, 5))
////    dataset.close()
//
//    val idxNew = indexColumn.readIndex()
////    println(idxNew)
//
//    val CIDs = (indexColumn.searchCID(idxNew, "Maya") ++
//    indexColumn.searchCID(idxNew, "Cyril") ++
//    indexColumn.searchCID(idxNew, "Bethel") ++
//    indexColumn.searchCID(idxNew, "Dorothy"))
//
//    println(CIDs.length)
//
//}