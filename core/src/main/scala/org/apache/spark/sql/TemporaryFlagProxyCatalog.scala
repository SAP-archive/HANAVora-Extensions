package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.execution.datasources.IsLogicalRelation
import org.apache.spark.sql.sources.Relation

/**
 * Enhances the Override Catalog to query returned datasource tables if they are temporary
 * or not. This requires the relations to extend the trait TemporaryFlagRelation.
 */
trait TemporaryFlagProxyCatalog extends OverrideCatalog{
  abstract override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val tables = super.getTables(databaseName)
    val proxiedTables = tables.map {
      case (tableName: String , isTemporary: Boolean) =>
        val tableIdentifier = TableIdentifier(tableName)
        lookupRelation(tableIdentifier) match {
          case sq @ Subquery(_, IsLogicalRelation(br: Relation)) =>
            (tableIdentifier.table, br.isTemporary)
          case _ => (tableIdentifier.table, isTemporary)
        }
      }
    proxiedTables
  }
}
