package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.util.PlanUtils._
import org.codehaus.groovy.ast.expr.AttributeExpression

import scala.collection.immutable.Queue

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
      case ((attr, (columnName, tableName)), index) =>
        // + 1 since ordinal should start at 1
        FieldExtractor(index + 1, tableName, columnName,
          attr.dataType, attr.metadata, attr.nullable, shouldCheckStar)
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

  def backTrack(attribute: Attribute): (String, String) = {
    val preOrderSeq = plan.toPreOrderSeq
    val (originalAttribute, attrNameOpt) =
      preOrderSeq.foldLeft[(Attribute, Option[String])](attribute -> None) {
        case ((attr, nameAlias), Reformatting(expressions)) =>
          val column = ColumnMatcher(attr.exprId)
          val updated = expressions.collectFirst {
            case alias@column(matched) =>
              alias.name -> matched
          }
          /** With this, only the top most alias (if any) is preserved) */
          updated.map(_._2).getOrElse(attr) -> nameAlias.orElse(updated.map(_._1))
        case (attr, default) =>
          attr
    }

    val attrName = attrNameOpt.getOrElse(attribute.name)

    val candidates = preOrderSeq.filter(_.outputSet.contains(originalAttribute)).reverse

    val nameCandidates = candidates.map(extractName)

    val tableName = nameCandidates.collectFirst {
      case Some(name) => name
    }.getOrElse(candidates.head.nodeName)

    attrName -> tableName
  }

  def extractName(plan: LogicalPlan): Option[String] = plan match {
    case Subquery(alias, _) => Some(alias)
    case _ => None
  }

  def tablePart: Seq[Any] = {
    tableSchema :: Nil
  }
}
