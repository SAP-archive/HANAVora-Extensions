package com.sap.spark.util

import java.util.Locale

import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SapSQLContext}
import org.apache.spark.sql.hive.SapHiveContext

import scala.util.{Failure, Success}

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

  def newSQLContext(sc: SparkContext): SQLContext = {
    if (TestUtils.getSetting("test.with.hive.context", "false") == "true") {
      new SapHiveContext(sc)
    } else {
      new SapSQLContext(sc)
    }

  }

  /**
    * Parses a PTest file.
    *
    * PTest files contain queries with expected results and optionally a parsed representation
    * of the query.
    *
    * @param fileName the name of the ptest file
    * @return as sequence of 3-tuples with "($query, $parsed, $expect)"
    */
  def parsePTestFile(fileName: String): List[(String, String, String)] = {
    val filePath = getFileFromClassPath(fileName)
    val fileContents = Source.fromFile(filePath).getLines
      .map(p => p.stripMargin.trim)
      .filter(p => !p.isEmpty && !p.startsWith("//")) // filter empty rows and comments
      .mkString("\n")
    val p = new PTestFileParser

    // strip semicolons from query and parsed
    p(fileContents) match {
      case Success(lines) =>
        lines.map {
          case (query, parsed, expect) =>
            (stripSemicolon(query).trim, stripSemicolon(parsed).trim, expect.trim)
        }
      case Failure(ex) => throw ex
    }
  }

  private def stripSemicolon(sql: String): String =
    if (sql.endsWith(";")) {
      sql.substring(0, sql.length-1)
    } else {
      sql
    }
}
