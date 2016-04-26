package immutable.encoders

import java.io._

import immutable._
import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.hashing.MurmurHash3

/**
  * Created by marcin on 2/26/16.
  */
trait Encoder {
    def loader(col: Column): Loader
    def iterator(col: Column): SeekableIterator[(Int, _)]

    trait SeekableIterator[A] extends Iterator[A] {
        def seek(pos: Int): Unit
        def next: A
        def hasNext: Boolean
        def position: Int
    }

    trait Loader {
        def write(data: Vector[String]): Unit
        def close: Unit
    }
}

trait EncoderDescriptor {
    def descriptor(col: Column): BlockDescriptor[_]

    trait BlockDescriptor[A] {
        var descriptors = Vector[A]()
        val col: Column

        def add(vec: Vector[String]): Unit

        def read: Vector[A] = {
            if (EncoderDescriptor.descriptors.contains(col.name)) {
                EncoderDescriptor.descriptors.get(col.name).get.asInstanceOf[Vector[A]]
            } else {
                val is = new ObjectInputStream(new FileInputStream(s"${Config.home}/${col.tblName}/${col.name}.descriptor"))
                val obj = is.readObject()
                is.close
                EncoderDescriptor.descriptors += (col.name -> obj.asInstanceOf[Vector[A]])
                EncoderDescriptor.descriptors.get(col.name).get.asInstanceOf[Vector[A]]
            }
        }

        def write = {
            val os = new ObjectOutputStream(new FileOutputStream(s"${Config.home}/${col.tblName}/${col.name}.descriptor"))
            os.writeObject(descriptors)
            os.close
        }
    }
}

object EncoderDescriptor {
    val descriptors = mutable.HashMap[String, Vector[_]]()
}

trait DescriptorType extends Serializable {

}

@SerialVersionUID(100L)
case class NumericDescriptor[A](val min: A, val max: A, val hist: (Array[Double], Array[Int])) extends Serializable

@SerialVersionUID(101L)
case class CharDescriptorNgram[A](val ngrams: mutable.HashMap[String, Int]) extends Serializable

@SerialVersionUID(102L)
case class CharDescriptorBloom[A](val filter: Bloom) extends Serializable

