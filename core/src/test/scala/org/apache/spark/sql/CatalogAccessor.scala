package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Object to access the [[Catalog]] of a [[SQLContext]].
  */
object CatalogAccessor {

  def lookupRelation(tableName: String, sqlContext: SQLContext): LogicalPlan =
    sqlContext.catalog.lookupRelation(TableIdentifier(tableName))
}
