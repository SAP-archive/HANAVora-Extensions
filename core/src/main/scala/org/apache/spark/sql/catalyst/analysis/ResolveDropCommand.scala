package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{DropRunnableCommand, LogicalRelation}
import org.apache.spark.sql.sources.DropRelation
import org.apache.spark.sql.sources.commands.{RelationKind, Table, UnresolvedDropCommand, WithExplicitRelationKind}

import scala.util.Try

/**
  * Resolves [[UnresolvedDropCommand]]s.
  */
case class ResolveDropCommand(analyzer: Analyzer, catalog: Catalog) extends Rule[LogicalPlan] {
  private def failAnalysis(reason: String) = throw new AnalysisException(reason)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case UnresolvedDropCommand(kind, allowNotExisting, tableIdent, cascade) =>
      val plan = resolvePlan(kind, tableIdent, allowNotExisting)

      val affected = plan.map { lp =>
        val targetKind =
          lp.collectFirst { case WithExplicitRelationKind(relationKind) => relationKind }
            .getOrElse(Table) // By default, we treat something as a table
        checkValidKind(kind, tableIdent, targetKind)
        buildDependentsMap(tableIdent)
      }

      affected.foreach(checkAllowedToDrop(cascade))
      DropRunnableCommand(affected.getOrElse(Map.empty))
  }

  private def getDropRelation(plan: LogicalPlan): Option[DropRelation] = plan.collectFirst {
    case d: LogicalPlan with DropRelation => d
    case LogicalRelation(d: DropRelation, _) => d
  }

  private def resolvePlan(kind: RelationKind,
                          tableIdent: TableIdentifier,
                          allowNotExisting: Boolean): Option[LogicalPlan] = {
    Try(catalog.lookupRelation(tableIdent)).toOption match {
      case Some(plan) => Some(plan)
      case None if allowNotExisting => None
      case None => failAnalysis(s"${kind.name} ${tableIdent.unquotedString} does not exist. To " +
        s"DROP a ${kind.name} regardless if it exists of not, use DROP ${kind.name} IF EXISTS.")
    }
  }

  private def checkAllowedToDrop(cascade: Boolean)
                                (dependents: Map[String, Option[DropRelation]]) = {
    if (dependents.size > 1 && !cascade) {
      failAnalysis("Can not drop because more than one relation has " +
        s"references to the target relation: ${dependents.keys.mkString(",")}. " +
        s"to force drop use 'CASCADE'.")
    }
  }

  private def checkValidKind(kind: RelationKind,
                             tableIdent: TableIdentifier,
                             targetKind: RelationKind): Unit = {
    if (targetKind != kind) {
      failAnalysis(s"Relation '${tableIdent.unquotedString} of kind" +
        s"$targetKind is not a ${kind.name}. Please use DROP ${targetKind.name.toUpperCase()} " +
        s"to drop it.")
    }
  }

  private def buildDependentsMap(identifier: TableIdentifier)
      : Map[String, Option[DropRelation]] = {
    val tables = getTables(identifier.database)
    val tablesAndDependents = buildDependentsMap(tables)

    def aggregate(acc: Set[TableIdentifier],
                  next: List[TableIdentifier]): Set[TableIdentifier] = next match {
      case Nil => acc
      case ident :: rest =>
        val dependents = tablesAndDependents(ident)
        aggregate(acc ++ dependents, rest ++ dependents.diff(acc))
    }

    val dependentsSet = aggregate(Set(identifier), identifier :: Nil)
    dependentsSet.flatMap { dependent =>
      tables.get(dependent).map(dependent.table -> getDropRelation(_))
    }.toMap
  }

  private def getTables(database: Option[String]): Map[TableIdentifier, LogicalPlan] =
    catalog
      .getTables(database)
      .map {
        case (name, _) =>
          val ident = TableIdentifier(name, database)
          val plan = catalog.lookupRelation(ident)
          ident -> plan
      }.toMap

  private def buildDependentsMap(tables: Map[TableIdentifier, LogicalPlan])
      : Map[TableIdentifier, Set[TableIdentifier]] = {
    /**
      * First, build up a map of table identifiers and the tables they
      * are referencing in their logical plans.
      */
    val tablesAndReferences = tables.mapValues(_.collect {
      case UnresolvedRelation(ident, _) => ident
    }.toSet)

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
