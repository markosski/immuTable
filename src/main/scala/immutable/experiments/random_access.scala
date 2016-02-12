package experiments

/**
 * Created by marcin on 8/14/15.
 */


import java.io.RandomAccessFile

case class Record(fname: String, age: Int, state: String, zip: String) {
    override def toString: String = {
        return fname + ", " + age.toString + ", " + state + ", " + zip
    }
}

object random_access extends App {

    def toInt(bytes: Array[Byte]): Int = bytes.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF))

//    def toByte(integer: Int): Array[Byte] =

    def fieldValue(file: RandomAccessFile, id: Int, dsize: Int) = {
        val data = new Array[Byte](dsize)
        file.seek(id * dsize)
        file.read(data)
        data
    }

    val file_age: RandomAccessFile = new RandomAccessFile("/tmp/data_binary_int.dat", "r")
    val file_fname: RandomAccessFile = new RandomAccessFile("/tmp/data_binary_fname.dat", "r")
    val file_state: RandomAccessFile = new RandomAccessFile("/tmp/data_binary_state.dat", "r")
    val file_zip: RandomAccessFile = new RandomAccessFile("/tmp/data_binary_zip.dat", "r")

    val file_meta: RandomAccessFile = new RandomAccessFile("/tmp/data_binary_meta.json", "r")
    val metaString = file_meta.readLine()
    println(metaString)
    file_meta.close()


    for (i <- 0 to 100) {
        val recordID = math.abs(scala.util.Random.nextInt() >> 16)

        val record_fname = fieldValue(file_fname, recordID, 12)
        val record_age = fieldValue(file_age, recordID, 1)
        val record_state = fieldValue(file_state, recordID, 2)
        val record_zip = fieldValue(file_zip, recordID, 5)

        val record = new Record(
            new String(record_fname),
            toInt(record_age),
            new String(record_state),
            new String(record_zip))
        println(record)
    }

    file_zip.close()
    file_state.close()
    file_age.close()
    file_fname.close()
}
