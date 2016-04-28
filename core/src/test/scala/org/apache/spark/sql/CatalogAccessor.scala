package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.Catalog

/**
  * Object to access the [[Catalog]] of a [[SQLContext]].
  */
object CatalogAccessor {
  def apply(sqlContext: SQLContext): Catalog =
    sqlContext.catalog
}
