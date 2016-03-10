package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.util.PlanUtils._

import scala.collection.immutable.Queue

/** Extracts informations from a given logical plan.
  *
  * @param plan The logical plan to extract information from.
  */
case class LogicalPlanExtractor(plan: LogicalPlan) {
  /** Intentionally left empty for now. */
  val tableSchema: String = ""

  lazy val columns: Seq[Seq[Any]] = {
    val attributes = plan.output
    val shouldCheckStar = plan.isInstanceOf[LogicalRelation]
    attributes.map { e =>
      Field.from(tableNameFor(e), e)
    }.zipWithIndex.flatMap {
      case (field, index) =>
        // + 1 since ordinal should start at 1
        FieldExtractor(index + 1, field, shouldCheckStar).extract()
    }
  }

  def tableNameFor(attribute: Attribute): String = {
    val preOrderSeq = plan.toPreOrderSeq
    val originalAttribute = preOrderSeq.foldLeft(attribute) {
      case (attr, Project(projectList, _)) =>
        projectList.collectFirst {
          case alias@Alias(child: Attribute, _) if alias.exprId == attr.exprId =>
            child
        }.getOrElse(attr)
      case (attr, default) =>
        attr
    }

    val candidates = preOrderSeq.filter(_.output.exists(_.contains(originalAttribute))).reverse

    val nameCandidates = candidates.map(extractName)

    nameCandidates.collectFirst {
      case Some(name) => name
    }.getOrElse(candidates.head.nodeName)
  }

  def extractName(plan: LogicalPlan): Option[String] = plan match {
    case Subquery(alias, _) => Some(alias)
    case LogicalRelation(r: SqlLikeRelation, _) => Some(r.tableName)
    case _ => None
  }

  def tablePart: Seq[Any] = {
    tableSchema :: Nil
  }

  def extract(): Seq[Seq[Any]] = {
    columns.map { row =>
       tablePart ++ row
    }
  }
}
