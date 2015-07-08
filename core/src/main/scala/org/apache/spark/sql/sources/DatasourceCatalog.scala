package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext

/**
 * This trait (that should be added to a DefaultSource/Datasource) indicates that the
 * data source also has a catalog which could possibly contain information about already present
 * elements.
 */
trait DatasourceCatalog {
  def getTableNames(sqlContext: SQLContext, parameters: Option[Map[String, String]]) : Seq[String]
}
