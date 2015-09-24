package com.sap.spark.util

import java.util.Locale

/**
 * Miscellaneous utilities for test suites.
 */
object TestUtils {

  /**
   * Get absolute path in the file system for a given path in the classpath.
   *
   * @param fileName Path in the classpath.
   * @return Absolute path in the filesystem.
   */
  def getFileFromClassPath(fileName: String): String =
    getClass.getResource(fileName).getPath.replaceAll("/C:", "")

  /**
   * Gets a setting from a system property, environment variable or default value. In that order.
   *
   * @param key Lowercase, dot-separated system property key.
   * @param default Optional default value.
   * @return Setting value.
   */
  def getSetting(key: String, default: String): String = {
    Seq(
      Option(System.getProperty(key)),
      Option(System.getenv(key.toUpperCase(Locale.ENGLISH).replaceAll("\\.", "_"))),
      Some(default)
    )
      .flatten
      .head
  }

}
