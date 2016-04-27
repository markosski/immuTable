package immutable.encoders

import java.nio.{IntBuffer, ByteBuffer}

import immutable._

/**
  * Created by marcin on 2/26/16.
  */
trait Encoder {
    def loader(col: Column): Loader
    def iterator(col: Column): SeekableIterator[Vector[_]]

    trait SeekableIterator[A] extends Iterator[A] {
        def seek(pos: Int): Unit
        def next: A
        def hasNext: Boolean
    }

    trait Loader {
        def load(data: Vector[String]): Unit
        def finish: Unit
    }
}



