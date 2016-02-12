package immutable

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Created by marcin on 2/5/16.
  */
object LoggerHelper {
    BasicConfigurator.configure()
    val log = Logger.getLogger("immutable")
    log.setLevel(Level.INFO)
    def info(message: String) = log.info(message)
    def warn(message: String) = log.warn(message)
    def error(message: String) = log.error(message)
    def debug(message: String) = log.debug(message)
}
