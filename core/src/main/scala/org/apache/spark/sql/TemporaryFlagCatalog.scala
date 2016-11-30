package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.Relation

/** Enhances a catalog to correctly mark [[Relation]]s as temporary or not. */
trait TemporaryFlagCatalog extends Catalog {
  abstract override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val tables = super.getTables(databaseName)
    tables.map {
      case (tableName: String , isTemporary: Boolean) =>
        val tableIdentifier = TableIdentifier(tableName)
        lookupRelation(tableIdentifier) match {
          case Subquery(_, LogicalRelation(relation: Relation, _)) =>
            (tableIdentifier.table, relation.isTemporary)
          case _ => (tableIdentifier.table, isTemporary)
        }
    }
  }
}

