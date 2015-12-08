package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.extension._

/**
 * [[SapSQLContext]] is the main entry point for SAP Spark extensions.
 * It is a drop-in replacement for [[SQLContext]].
 * Users of this class should check Apache Spark SQL official documentation.
 *
 * This context provides:
 *
 *  - A new data source API that can be used to push arbitrary queries down to the data source.
 *    See [[org.apache.spark.sql.sources.CatalystSource]].
 *  - Support for a new SQL extension for hierarchy queries.
 *  - New DDL commands (e.g. REGISTER TABLE).
 *  - Support for both temporary and non-temporary tables.
 *
 * @see [[SQLContext]].
 * @see [[org.apache.spark.sql.hive.SapHiveContext]]
 * @since 1.0
 */
class SapSQLContext(@transient override val sparkContext: SparkContext)
  extends ExtendableSQLContext(sparkContext)
  with CommonSapSQLContext
