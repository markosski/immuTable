package immutable

/**
  * Created by marcin on 2/22/16.
  */
object Config {
    val home = "/Users/marcin/correla"
    val readBufferSize = 4096
    val loggerLevel = 'DEBUG
    val vectorSize = 256

    object bulkLoad {
        val vectorSize = 8192
    }

    object Descriptors {
        val enable = false
    }
}
