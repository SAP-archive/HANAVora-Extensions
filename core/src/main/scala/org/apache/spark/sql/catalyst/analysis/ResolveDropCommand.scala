package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.commands.UnresolvedDropCommand
import org.apache.spark.sql.sources.{DropRelation, RelationKind, Table}

import scala.util.Try

/**
  * Resolves [[UnresolvedDropCommand]]s.
  */
case class ResolveDropCommand(analyzer: Analyzer, catalog: Catalog)
  extends Rule[LogicalPlan]
  with TableDependencyCalculator {

  private def failAnalysis(reason: String) = throw new AnalysisException(reason)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case UnresolvedDropCommand(kind, allowNotExisting, tableIdent, cascade) =>
      val tableId = alterByCatalystSettings(catalog, tableIdent)
      val plan = resolvePlan(kind, tableId, allowNotExisting)

      val affected = plan.map { lp =>
        val targetKind =
          RelationKind.kindOf(lp, Table)
        checkValidKind(kind, tableId, targetKind)
        buildDependentsMap(catalog, tableId)
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

  private def buildDependentsMap(catalog: Catalog, identifier: TableIdentifier)
  : Map[String, Option[DropRelation]] = {
    val tables = getTables(catalog, identifier.database)
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
}
