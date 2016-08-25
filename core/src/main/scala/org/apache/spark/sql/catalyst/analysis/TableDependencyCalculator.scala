package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.CaseSensitivityUtils._

/**
  * The functionality of calculating the dependencies for the tables in a given
  * catalog and database.
  *
  * The calculation of referencing tables is subject to case sensitivity.
  */
trait TableDependencyCalculator {
  protected def getTables(catalog: Catalog,
                          database: Option[String] = None): Map[TableIdentifier, LogicalPlan] =
    catalog
      .getTables(database)
      .map {
        case (name, _) =>
          val ident = catalog.fixCase(TableIdentifier(name, database))
          val plan = catalog.lookupRelation(ident)
          ident -> plan
      }.toMap

  /**
    * Constructs a map of [[TableIdentifier]]s and their dependent [[TableIdentifier]]s.
    *
    * @param tables A map of [[TableIdentifier]]s and their associated [[LogicalPlan]]s.
    * @return A map of [[TableIdentifier]]s and a set of [[TableIdentifier]]s that
    *         have dependencies to it.
    */
  protected def buildDependentsMap[A: CaseSensitivitySource](
                                        tables: Map[TableIdentifier, LogicalPlan],
                                        source: A): Map[TableIdentifier, Set[TableIdentifier]] = {
    /**
      * First, build up a map of table identifiers and the tables they
      * are referencing in their logical plans.
      */
    val tablesAndReferences = tables.map {
      case (key, value) =>
        source.fixCase(key) -> value.collect {
          case UnresolvedRelation(ident, _) => source.fixCase(ident)
        }.toSet
    }

    /**
      * Then, iterate over all tables and the tables they are referencing. We know
      * that if for instance table a references table b, that means table b has
      * table a as dependent relation.
      */
    tablesAndReferences.foldLeft(Map.empty[TableIdentifier, Set[TableIdentifier]]
      .withDefaultValue(Set.empty)) {
      case (acc, (ident, references)) =>
        references.foldLeft(acc) {
          case (innerAcc, referenceIdentifier) =>
            innerAcc + (referenceIdentifier -> (innerAcc(referenceIdentifier) + ident))
        }
    }
  }
}
