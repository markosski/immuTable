
import java.lang

var l: Long = 0

l |= (1.toLong << 63)
l |= (1.toLong << 62)
l |= (1.toLong << 1)
l |= (1.toLong << 0)

lang.Long.toBinaryString(l)

