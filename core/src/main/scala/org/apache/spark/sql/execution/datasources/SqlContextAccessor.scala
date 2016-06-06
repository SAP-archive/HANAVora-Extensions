package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.language.implicitConversions

/**
 * The purpose of this object is to provide a temporary access to the catalog so
 * we can write logical plans without any analysis which is important since some
 * plans could be resolved too early as it happened for views (bug 104634).
 */
object SqlContextAccessor {
  implicit def sqlContextToCatalogAccessable(sqlContext: SQLContext): SqlContextCatalogAccessor =
    new SqlContextCatalogAccessor(sqlContext)

  class SqlContextCatalogAccessor(sqlContext: SQLContext)
    extends SQLContext(sqlContext.sparkContext) {

    def registerRawPlan(lp: LogicalPlan, tableName: String): Unit = {
      sqlContext.catalog.registerTable(TableIdentifier(tableName), lp)
    }
  }
}
