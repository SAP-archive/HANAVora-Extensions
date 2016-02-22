package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions.{NamedExpression, ExprId, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.IsLogicalRelation
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.util.GenericUtil.RichGeneric

/** Extracts informations from a given logical plan.
  *
  * @param plan The logical plan to extract information from.
  */
case class LogicalPlanExtractor(plan: LogicalPlan) {
  /** Intentionally left empty for now. */
  val tableSchema: String = ""

  private object ExtractAttributes {
    def unapply(plan: LogicalPlan): Option[(Seq[NamedExpression], Boolean)] = plan matchOptional {
      case Project(projectList, _) => (projectList, true)
      case lr@IsLogicalRelation(_) => (lr.output, false)
    }
  }

  lazy val columns: Seq[Seq[Any]] = {
    val expressionIdMap = collectExpressionIdMap(plan)
    def tableNameFor(id: ExprId): String = expressionIdMap.getOrElse(id, {
      throw new Exception(s"NO ENTRY FOR $id, $expressionIdMap")
      "UNKNOWN"
    })

    plan match {
      case ExtractAttributes(expressions, checkStar) =>
        expressions.map { e =>
          Field.from(tableNameFor(e.exprId), e)
        }.zipWithIndex.flatMap {
          case (field, index) =>
            FieldExtractor(index, field, checkStar).extract()
        }
    }
  }

  /** Aggregates the table names with the corresponding expression ids
    *
    * @param plan The plan to traverse
    * @return The relations between table identifiers and expression ids
    */
  private def collectExpressionIdMap(plan: LogicalPlan): Map[ExprId, String] = plan.collect {
    case s@Subquery(alias, _) =>
      s.output.map(out => (out.exprId, alias))

    case lr@IsLogicalRelation(r: SqlLikeRelation) =>
      lr.output.map(out => (out.exprId, r.tableName))

    case h: Hierarchy =>
      h.parenthoodExpression.references flatMap {
        case a: Attribute if h.child.output.exists(_.exprId == a.exprId) =>
          None
        case a =>
          Some(a.exprId -> h.childAlias)
      }
  }.flatten.reverse.toMap

  def tablePart: Seq[Any] = {
    tableSchema :: Nil
  }

  def extract(): Seq[Seq[Any]] = {
    columns.map { row =>
       tablePart ++ row
    }
  }
}
