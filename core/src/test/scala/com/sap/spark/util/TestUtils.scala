package com.sap.spark.util

import java.util.Locale

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Row, SQLContext, SapSQLContext}
import org.apache.spark.sql.hive.SapHiveContext
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.sources.{BaseRelation, CatalystSource, Table}
import org.apache.spark.sql.types.StructType
import org.mockito.Matchers._
import org.mockito.Mockito._

import scala.tools.nsc.io.Directory
import scala.util.{Failure, Success}

/**
 * Miscellaneous utilities for test suites.
 */
object TestUtils {

  abstract class MockRelation
    extends BaseRelation
    with Table
    with SqlLikeRelation
    with CatalystSource

  /** Registers a [[org.apache.spark.sql.DataFrame]] as a
    * catalyst relation to the sqlContext
    *
    * @param tableName The name of the registered table
    * @param schema The schema of the data frame
    * @param data The rdd of the data frame
    * @param sqlc The sqlContext to register the data frame to
    * @return The created relation that is registered to the sqlContext
    */
  def registerMockCatalystRelation(tableName: String,
                                   schema: StructType,
                                   data: RDD[Row])
                                  (implicit sqlc: SQLContext): MockRelation = {
    val relation = mock(classOf[MockRelation])
    when(relation.supportsLogicalPlan(any[LogicalPlan])).thenReturn(true)
    when(relation.logicalPlanToRDD(any[LogicalPlan])).thenReturn(data)
    when(relation.schema).thenReturn(schema)
    when(relation.isMultiplePartitionExecution(any[Seq[CatalystSource]]))
      .thenReturn(true)
    sqlc.baseRelationToDataFrame(relation).registerTempTable(tableName)
    relation
  }

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

  def withTempDirectory[A](f: Directory => A): A = {
    val dir = Directory.makeTemp()
    try {
      f(dir)
    } finally {
      dir.deleteIfExists()
    }
  }
}
