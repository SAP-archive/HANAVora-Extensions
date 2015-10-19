package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.sources.{LogicalRelation, TemporaryFlagRelation}

/**
 * Enhances the Override Catalog to query returned datasource tables if they are temporary
 * or not. This requires the relations to extend the trait TemporaryFlagRelation.
 */
trait TemporaryFlagProxyCatalog extends OverrideCatalog{
  abstract override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val tables = super.getTables(databaseName)

    val proxiedTables = tables.map {
      case (tableName : String , isTemporary : Boolean) => {
        lookupRelation(Seq(tableName)) match {
          case sq @ Subquery(_, lr @ LogicalRelation(br : TemporaryFlagRelation)) =>
            (tableName, br.isTemporary())
          case _ => (tableName, isTemporary)
        }
      }
    }
    proxiedTables
  }
}
