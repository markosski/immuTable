package main.scala.immutable.experiments

import java.io.RandomAccessFile
import java.nio.{MappedByteBuffer, ByteBuffer}
import java.nio.channels.FileChannel
import java.util.Date

import main.scala.immutable.BufferManager

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{DurationConversions, Duration}
import scala.util.{Success, Failure, Try}
import main.scala.immutable.helpers.Conversions._
import java.util.Arrays
import scala.collection.mutable.ArrayBuffer

class RBV(size: Int, predicate: (Byte) => Boolean ) {
    private val result = ByteBuffer.allocate((size/8) + 8)
    var inter = new ArrayBuffer[Byte]()

    def add(value: Byte) = {
        if (inter.size < 8) {
            inter += value
        } else {
            var resByte = 0
            var i = 0
            while (i < 8) {
                if (predicate(inter(i))) resByte += math.pow(2, i).toInt
                i += 1
            }
            result.put(resByte.toByte)

            inter = new ArrayBuffer[Byte]()
            inter += value
        }
    }

    def get = {
        result.flip()
        result.asReadOnlyBuffer()
    }
}

/**
 * Created by marcin on 11/8/15.
 */
object SingleVSManyThreadScan extends App {
    def readSingleBuffered(tableName: String, columnName: String) = {
        val start = new Date().getTime()
        val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r")
        val size = 100000000
        // val colFileMapped = colFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, colFile.length)

        val stageResult = ByteBuffer.allocate(size)
        val pageSize = 4096
        val colBytes = new Array[Byte](4)
        val pageBytes = new Array[Byte](pageSize)
        val buffer = ByteBuffer.allocate(pageSize + 8)
        var counter = 0
        while (counter < size / pageSize) {
            colFile.read(pageBytes)
            buffer.put(pageBytes)
            buffer.flip()
            while (buffer.position < pageSize) {
                val value = buffer.get()
                if (value == 34) stageResult.put(value)
            } // get Byte)
            buffer.clear()
            counter += 1
        }

        println("done: " + (new Date().getTime() - start).toString + "ms")
        buffer.clear()
        colFile.close()
        println(stageResult.position)
    }

    def readSinglePassNoBuffer(tableName: String, columnName: String) = {
        val start = new Date().getTime()
        val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r")
        val size = 100000000

        val stageResult = new ArrayBuffer[Byte]()
        val colBytes = new Array[Byte](4)
        var counter = 0
        while (counter < size) {
            val value = colFile.readByte()
            if (value == 50) stageResult += value
            counter += 1
        }

        println("done: " + (new Date().getTime() - start).toString + "ms")
        println(stageResult.size)
        colFile.close()
    }

    def readSingleBufferedNoMmapMultiThread(tableName: String, columnName: String) = {
        val cores = 4
        val size = 100000000
        val futures = mutable.MutableList[Future[ArrayBuffer[Byte]]]()
        def stage(begin: Int, offset: Int) = {
            val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r")
            colFile.seek(begin)

            val stageResult = new ArrayBuffer[Byte]()
            val pageSize = 4096
            val colBytes = new Array[Byte](4)
            val pageBytes = new Array[Byte](pageSize)
            val buffer = ByteBuffer.allocate(pageSize + 8)
            var counter = 0
            while (counter < offset / pageSize) {
                colFile.read(pageBytes)
                buffer.put(pageBytes)
                buffer.flip()
                while (buffer.position < pageSize) {
                    val value = buffer.get()
                    if (value == 50) stageResult += value
                } // get Byte)
                buffer.clear()
                counter += 1
            }
            buffer.clear()
            colFile.close()
            println(stageResult.size)
            stageResult
        }

        for (i <- 0 until cores) futures += Future(stage(i * size / cores, size / cores))

        val start = new Date().getTime()
        val futRes = Future.sequence(futures)
        Await.ready(futRes, Duration.Inf)
        val res = futRes onComplete(x => x match {
            case Success(x) => x.reduce((a, b) => a ++=: b)
            case Failure(e) => throw new Exception(e)
        })
        println(res)
        println("done: " + (new Date().getTime() - start).toString + "ms")
    }

    def readSingleBufferedMultiThread(tableName: String, columnName: String) = {
        val cores = 4
        val size = 100000000
        val threads = mutable.MutableList[Thread]()
        val futures = mutable.MutableList[Future[ArrayBuffer[Byte]]]()
        val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r")
        val filed = colFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, size)

        def stage(begin: Int, offset: Int) = {
            val stageResult = new ArrayBuffer[Byte]()

            val pageSize = 4096
            var counter = 0
            var counterInner = 0
            val value = new Array[Byte](1)

            while (counter < offset) {
                filed.position(begin + counter)
                val value = filed.get()
                if (value == 50) stageResult += value
                counter += 1
                counterInner += 1
            }

            println(stageResult.size)
            println(counterInner)
            stageResult
        }
        val start = new Date().getTime()
        for (i <- 0 until cores) futures += Future(stage(i * size / cores, size / cores))

        val futRes = Future.sequence(futures)
        Await.ready(futRes, Duration.Inf)
        val res = futRes onComplete(x => x match {
            case Success(x) => x // x.reduce((a, b) => a ++=: b)
            case Failure(e) => throw new Exception(e)
        })

        colFile.close()
        //        filed.clear()
        //        println(res)
        println("done: " + (new Date().getTime() - start).toString + "ms")
    }

    def readSingleMultiThread(tableName: String, columnName: String) = {
        val cores = 4
        val size = 100000000
        val threads = mutable.MutableList[Thread]()
        val futures = mutable.MutableList[Future[ArrayBuffer[Byte]]]()
        val colFile = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r")
        val filed = colFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, size)
        filed.load()

        def stage(begin: Int, offset: Int) = {
            val stageResult = new ArrayBuffer[Byte]()

            val pageSize = 4096
            var counter = 0
            var counterInner = 0
            val value = new Array[Byte](1)

            while (counter < offset) {
                filed.position(begin + counter)
                val value = filed.get()
                if (value == 50) stageResult += value
                counter += 1
                counterInner += 1
            }

            println(stageResult.size)
            println(counterInner)
            stageResult
        }
        val start = new Date().getTime()
//        stage(0, size)
        for (i <- 0 until cores) futures += Future(stage(i * size / cores, size / cores))

        val futRes = Future.sequence(futures)
        Await.ready(futRes, Duration.Inf)
//        val res = futRes onComplete(x => x match {
//            case Success(x) => x // x.reduce((a, b) => a ++=: b)
//            case Failure(e) => throw new Exception(e)
//        })
        colFile.close()
//        filed.clear()
//        println(res)
        println("done: " + (new Date().getTime() - start).toString + "ms")
    }

    def readWithBufferManager(tableName: String, columnName: String) = {
        val futures = mutable.MutableList[Future[ByteBuffer]]()
        val bf = new BufferManager()
        bf.registerFromFile(columnName, s"/Users/marcin/correla/$tableName/$columnName.col", 4)

        def run = {
            def stage(mmap: MappedByteBuffer) = {
//                val stageResult = ArrayBuffer[Int]()
                val stageResult = ByteBuffer.allocate(100000000)
                val chunk = new Array[Byte](1)

                var counterInner = 0
                val testValue = Array[Byte](34)
//                val rbv = new RBV(100000000, (value: Byte) => if (value == 34.toByte) true else false)
                while (mmap.hasRemaining) {
                    mmap.get(chunk)

//                    if (Arrays.equals(chunk, testValue)) stageResult.put(chunk(0))

//                    rbv.add(chunk(0))
                    counterInner += 1
                }
                stageResult.limit(stageResult.position)
                println(stageResult.position)
//                println(counterInner)
//                println("result " + rbv.get)
                mmap.position(0)
                stageResult
            }

            val start = new Date().getTime()
            for (mmap <- bf.getAll(columnName)) futures += Future(stage(mmap))
            val futRes = Future.sequence(futures)
            Await.ready(futRes, Duration.Inf)
//            stage(bf.get(columnName)(0))

            println("done: " + (new Date().getTime() - start).toString + "ms")
        }

        for (i <- 0 until 4) run
    }

    def readMemoryOnly(tableName: String, columnName: String) = {
        val size = 100000000
        val ch = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r").getChannel
        val buffer = ByteBuffer.allocate(size)
        val stageResult = ArrayBuffer[Int]()

        var counterInner = 0
        while (ch.read(buffer) != -1)

        buffer.position(0)
        val chunk = new Array[Byte](1)
        val testValue = Array[Byte](34)

        var counter = 0

        val start = new Date().getTime()
        while (buffer.hasRemaining && buffer.position < size - 1) {
            buffer.get(chunk)
            if (chunk(0) == testValue(0)) counterInner += 1
            counter += 1
        }
        println("done: " + (new Date().getTime() - start).toString + "ms")
        println("size: " + counterInner)
        buffer.clear
    }

    def readMemoryOnlyFixedArray(tableName: String, columnName: String) = {
        val size = 10000000
        val dataSize = 1
        val ch = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r").getChannel
        val array = new Array[Byte](size * dataSize)
        val buffer = ByteBuffer.allocate(size * dataSize)
        val stageResult = ArrayBuffer[Int]()
        var bytes = new Array[Byte](dataSize)

        while (ch.read(buffer) != -1)
        buffer.position(0)

        var counter = 0
        while (buffer.hasRemaining) {
            array(counter) = buffer.get()
            counter += 1
        }

        def getTester(size: Int): (Array[Byte], Array[Byte], Int) => Boolean = {
            if (size == 1) {
                (array: Array[Byte], testValue: Array[Byte], counter: Int) => if (array(counter) == testValue(0)) true else false
            } else if (size == 2) {
                (array: Array[Byte], testValue: Array[Byte], counter: Int) => if(array(counter) == testValue(0) && array(counter+1) == testValue(1)) true else false
            } else {
                throw new Exception("Such size does not exist")
            }
        }

        val tester = getTester(dataSize)

        counter = 0
//        val testValue = Array[Byte](67, 84)
        val testValue = Array[Byte](34)
        var counterInner = 0
        val start = new Date().getTime()
        while (counter < array.length / dataSize) {
//            if (Arrays.equals(Array[Byte](array(counter)), testValue)) counterInner += 1
            if (array(counter) == testValue(0)) counterInner += 1
//            if (tester(array, testValue, counter)) counterInner += 1
            counter += 1
        }
        println("done: " + (new Date().getTime() - start).toString + "ms")
        println("size: " + counterInner)
        buffer.clear
    }

    def readMemoryOnlyArray(tableName: String, columnName: String) = {
        val size = 10000000
        val ch = new RandomAccessFile(f"/Users/marcin/correla/$tableName%s/$columnName%s.col", "r").getChannel
        val array = new Array[Array[Byte]](size)
        val buffer = ByteBuffer.allocate(size)
        val stageResult = ArrayBuffer[Int]()
        var bytes = new Array[Byte](1)

        var counter = 0
        while (ch.read(buffer) != -1)
        buffer.position(0)

        while (buffer.hasRemaining) {
            bytes = new Array[Byte](1)
            buffer.get(bytes)
            array(counter) = bytes
            counter += 1
        }

        var counterInner = 0
//        val testValue = 34
        val testValue = Array[Byte](34)
//        val testValue = Array[Byte](67, 84)
        val start = new Date().getTime()

        counter = 0
        while (counter < array.length) {
//            if (array(counter)(0) == testValue(0)) counterInner += 1
            if (Arrays.equals(array(counter), testValue)) counterInner += 1
            counter += 1
        }
        println("done: " + (new Date().getTime() - start).toString + "ms")
        println("size: " + counterInner)
        buffer.clear
    }

//    readSingleBuffered("test_table_100mil", "age")
//    readSinglePassNoBuffer("test_table_100mil", "age")

//    readSingleBufferedMultiThread("test_table_100mil", "age")
//    readSingleBufferedMultiThread("test_table_100mil", "age")
//    readSingleBufferedNoMmapMultiThread("test_table_100mil", "age")
//    readSingleBufferedNoMmapMultiThread("test_table_100mil", "age")
    readMemoryOnlyFixedArray("test_table_100mil", "age")
//    readMemoryOnlyArray("test_table_100mil", "age")
//    readWithBufferManager("test_table_100mil", "age")
//
}
