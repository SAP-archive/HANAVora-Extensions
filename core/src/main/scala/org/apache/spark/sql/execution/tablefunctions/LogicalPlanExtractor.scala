package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.util.PlanUtils._

/** Extracts information from a given logical plan.
  *
  * @param plan The logical plan to extract information from.
  */
case class LogicalPlanExtractor(plan: LogicalPlan) {
  /** Intentionally left empty for now. */
  val tableSchema: String = ""

  lazy val columns: Seq[FieldExtractor] = {
    val attributes = plan.output
    val shouldCheckStar = !plan.isInstanceOf[LogicalRelation]
    attributes.map(attr => attr -> backTrack(attr)).zipWithIndex.map {
      case ((attr,
        (tableName, columnName, originalTableName, originalColumnName)), index) =>
        // + 1 since ordinal should start at 1
        FieldExtractor(
          index + 1,
          tableName,
          originalTableName,
          columnName,
          originalColumnName,
          attr.dataType,
          attr.metadata,
          attr.nullable,
          shouldCheckStar)
    }
  }

  case class ColumnMatcher(exprId: ExprId) {
    def unapply(alias: Alias): Option[Attribute] = {
      if (alias.exprId == exprId) {
        /**
          * We want to filter for all attributes in the child expression.
          * If there is only one attribute, then it is a certain match.
          * Otherwise, for more references it would be ambiguous and we
          * will return [[None]]
          */
        alias.child.collect {
          case attribute: Attribute => attribute
        }.distinct match {
          case Seq(reference) => Some(reference)
          case _ => None
        }
      } else {
        None
      }
    }
  }

  object Reformatting {
    def unapply(arg: LogicalPlan): Option[Seq[NamedExpression]] = arg match {
      case Project(projectList, _) => Some(projectList)
      case Aggregate(_, aggregateExpressions, _) => Some(aggregateExpressions)
      case _ => None
    }
  }

  private def backTrack(attribute: Attribute): (String, String, String, String) = {
    val preOrderSeq = plan.toPreOrderSeq
    val originalAttribute =
      preOrderSeq.foldLeft[Attribute](attribute) {
        case (attr, Reformatting(expressions)) =>
          val column = ColumnMatcher(attr.exprId)
          val updated = expressions.collectFirst {
            case alias@column(matched) => matched
          }
          /** With this, only the top most alias (if any) is preserved) */
          updated.getOrElse(attr)
        case (attr, default) =>
          attr
    }

    val originalTableName = extractName(originalAttribute, preOrderSeq.reverse).getOrElse("")
    val tableName = extractName(attribute, preOrderSeq).getOrElse("")

    (tableName, attribute.name, originalTableName, originalAttribute.name)
  }

  private def extractName(attribute: Attribute, plans: Seq[LogicalPlan]): Option[String] =
    plans.filter(_.outputSet.contains(attribute)).collectFirst {
      case Subquery(alias, _) => alias
      case LogicalRelation(r: SqlLikeRelation, _) => r.tableName
    }

  def tablePart: Seq[Any] = {
    tableSchema :: Nil
  }
}
