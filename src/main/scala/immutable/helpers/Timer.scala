package immutable.helpers

import java.util.Date

/**
  * Created by marcin on 2/27/16.
  */
case class Timer() {
    val time = new Date().getTime

    def get(prepend: String): String = {
        s"${prepend} in: " + (new Date().getTime - time).toString + "ms"
    }
}
