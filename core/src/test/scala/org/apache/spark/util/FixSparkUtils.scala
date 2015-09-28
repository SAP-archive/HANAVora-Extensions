package org.apache.spark.util

object FixSparkUtils {

  def setCustomHostname(hostname: String): Unit =
    Utils.setCustomHostname(hostname)

}
