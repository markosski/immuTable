package immutable

import java.io.RandomAccessFile
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
 * Created by marcin on 11/22/15.
 *
 */

abstract class ColumnBuffer {
    def position: Int
    def position(pos: Int): Unit
    def get: Byte
    def get(arr: Array[Byte]): Unit
    def hasRemaining: Boolean
}

class MMapColumnBuffer(buffer: ByteBuffer) extends ColumnBuffer {
    def position = buffer.position
    def position(pos: Int) = buffer.position(pos)
    def get = buffer.get
    def get(arr: Array[Byte]) = buffer.get(arr)
    def hasRemaining = buffer.hasRemaining
}

class MemoryColumnBuffer(buffer: ByteBuffer) extends ColumnBuffer {
    def position = buffer.position
    def position(pos: Int) = buffer.position(pos)
    def get = buffer.get
    def get(arr: Array[Byte]) = buffer.get(arr)
    def hasRemaining = buffer.hasRemaining
}

//class MemoryColumnBuffer extends ColumnBuffer {
//
//}

// TODO: Instead of buffer name should we take column ref. or it is better to keep buffer dumb
object ColumnBufferManager {
    val mbuffers: mutable.Map[String, Seq[ByteBuffer]] = mutable.Map()

    /**
     *
     * @param name Name of the buffer
     * @param filePath Location of file in filesystem
     * @param dataSize Byte size of data unit
     * @param parts Divide into even parts (or close to even) to use when processing by many threads
     */
    def registerMmap(name: String, filePath: String, dataSize: Int, parts: Option[Int] = Some(1)): Unit = {
        val threads: Int = parts match {
            case Some(parts) => parts
            case None => Runtime.getRuntime.availableProcessors
        }
        val file = new RandomAccessFile(filePath, "r")
        val size: Long = file.length
        def chunkSize(increase: Int): Long = if ((size + increase / threads) % dataSize != 0) chunkSize(increase + 1) else (size + increase) / threads

        // Create a sequence of chunk thresholds
        // return: Seq(25, 25, 25, 25)
        val chunks: Seq[Long] = {
            val head = for (i <- 0 until threads - 1) yield chunkSize(0)
            head ++ Seq(size - head.sum)
        }

        // Create mmap buffers based on chunk thresholds
        // following should generate mmaps with values like (0, 25), (25, 50), (50, 75), (75, 100)
        mbuffers += (name -> { for (i <- 0 until threads) yield file.getChannel().map(FileChannel.MapMode.READ_ONLY, chunks.take(i).sum, chunks(i)) })
//        for (buffer <- mbuffers; bufferPart <- buffer._2) bufferPart.load()
        file.close()
    }

    def registerMemory(name: String, filePath: String, dataSize: Int, parts: Option[Int] = Some(1)): Unit = {
        val file = new RandomAccessFile(filePath, "r")
        val chan = file.getChannel
        val buffer = ByteBuffer.allocate(chan.size.toInt)
        chan.read(buffer)
        buffer.flip()
        mbuffers += (name -> List(buffer))
    }

    /**
     * Get the buffer sequence.
      *
      * @param name Name of buffer
     * @return
     */
    def getAll(name: String): Seq[ByteBuffer] = mbuffers(name)

    /**
     * Get buffer by index. By default buffer at index 0.
      *
      * @param name
     * @return
     */
    def get(name: String, index: Int = 0): ByteBuffer = {
        Try(mbuffers(name)(index)) match {
            case Success(x) => x.duplicate;
            case Failure(e) => throw new Exception(s"Buffer ${name} was not found.")
        }
    }
}
