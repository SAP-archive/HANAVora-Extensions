package org.apache.spark.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.sql.AbstractSapSQLContext

/**
 * This context provides extended [[HiveContext]] functionality such as hierarchies, enhanced data
 * sources API with support for aggregates pushdown, etc.
 *
 * @see [[org.apache.spark.sql.AbstractSapSQLContext]]
 */
class SapHiveContext(@transient override val sparkContext: SparkContext)
  extends ExtendableHiveContext(sparkContext)
  with AbstractSapSQLContext
