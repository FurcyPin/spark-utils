package fpin.spark.utils

import org.apache.log4j.{LogManager, Logger}
import org.scalatest.freespec.AnyFreeSpec

class LoggingTest extends AnyFreeSpec {

  "test" in {
    Toto.log()
    HeritedToto.log()
  }

}

trait Logging extends Serializable {
  @transient
  protected lazy val logger: Logger = LogManager.getLogger(this.getClass)

  def logInfo(s: =>String): Unit = logger.info(s)
  def logInfo(s: =>String, e:Throwable): Unit = logger.info(s,e)

  def logWarning(s: =>String): Unit = logger.warn(s)
  def logWarning(s: =>String, e:Throwable): Unit = logger.warn(s,e)

  def logError(s:String): Unit = logger.error(s)
  def logError(s:String, e:Throwable): Unit = logger.error(s,e)

  def logDebug(s:String): Unit = logger.debug(s)
  def logDebug(s:String, e:Throwable): Unit = logger.debug(s,e)

}

object Toto extends Logging {
  def log(): Unit = {
    logger.error("error")
  }
}

object HeritedToto extends AbstractToto

abstract class AbstractToto extends Logging {

  def log(): Unit = {
    logger.error("error")
  }

}