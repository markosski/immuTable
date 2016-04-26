
import immutable._

val bloom = new Bloom(8196, 0.1)

bloom.hashIter
bloom.bitSize

bloom.add("marcin")
bloom.add("tomek")
bloom.add("melissa")

bloom.contains("marcin")
