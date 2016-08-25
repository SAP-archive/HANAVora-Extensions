package org.apache.spark.util

import org.apache.spark.sql.SQLContext

/**
  * Utility functions for running an operation with some configuration set and restoring afterwards.
  */
trait SqlContextConfigurationUtils {
  def sqlContext: SQLContext

  /**
    * Applies the given key-value setting to the settings of the sqlContext within the operation.
    *
    * @param key The key of the setting.
    * @param value The value of the setting.
    * @param a The operation.
    * @tparam A The result type of the operation.
    * @return The result of the operation.
    */
  def withConf[A](key: String, value: String)(a: => A): A =
    withConf(Map(key -> value))(a)

  /**
    * Applies the given configuration map to the settings of the sqlContext within the operation.
    *
    * @param settings The settings to apply.
    * @param a The operation.
    * @tparam A The result type of the operation.
    * @return The result of the operation.
    */
  def withConf[A](settings: Map[String, String])(a: => A): A = {
    val temps: Map[String, String] =
      settings.keys.map(key => key -> sqlContext.getConf(key))(scala.collection.breakOut)

    try {
      settings.foreach {
        case (key, value) => sqlContext.setConf(key, value)
      }
      a
    } finally {
      temps.foreach {
        case (key, value) => sqlContext.setConf(key, value)
      }
    }
  }
}
