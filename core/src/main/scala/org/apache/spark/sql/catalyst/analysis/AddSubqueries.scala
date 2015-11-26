package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.sources.sql.{SingleQuery, SqlLikeRelation}

/**
 * Adds subqueries where needed for SQL generation.
 */
object AddSubqueries extends Rule[LogicalPlan] {

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val planWithSubqueries = transform(plan)
    planWithSubqueries transformUp {
      case Subquery(name, Subquery(innerName, child)) =>
        /* If multiple subqueries, preserve the inner one */
        logDebug(s"Nested subqueries ($name, $innerName) -> $innerName")
        Subquery(innerName, child)
    } match {
      /* Avoid outer subquery */
      case Subquery(name, child) => child
      case other => other
    }
  }

  private def isSetOperation(lp: LogicalPlan): Boolean = lp match {
    case _: Union | _: Except | _: Intersect => true
    case _ => false
  }

  private[this] def getName(plan: LogicalPlan): Option[String] =
    plan match {
      case LogicalRelation(table: SqlLikeRelation) => Some(table.tableName)
      case Subquery(alias, child) => Some(alias)
      case _ => None
    }

  private def newIds(): Iterator[Int] = Stream.from(1).iterator

  private[this] def newTableName(ids: Iterator[Int]): String = s"__table${ids.next()}"

  private[this] def newQueryName(ids: Iterator[Int]): String = s"__subquery${ids.next()}"

  private[this] def transform(plan: LogicalPlan, ids: Iterator[Int] = newIds()): LogicalPlan =
    plan match {
      case relation@LogicalRelation(_: SqlLikeRelation) => relation
      case Subquery(alias, child) => Subquery(alias, transform(child, ids))
      case Distinct(union@Union(left, right)) =>
        Distinct(transform(union))
      case Join(left, right, joinType, condition) =>
        val leftName = getName(left)
        val rightName = getName(right)
        val newLeft = if (leftName.isEmpty || rightName == leftName) {
          Subquery(newQueryName(ids), transform(left, ids))
        } else {
          transform(left, ids)
        }
        val newRight = if (rightName.isEmpty || rightName == leftName) {
          Subquery(newQueryName(ids), transform(right, ids))
        } else {
          transform(right, ids)
        }
        Join(newLeft, newRight, joinType, condition)
      case hierarchy: Hierarchy =>
        val alias = newQueryName(ids)
        logTrace(s"Added subquery $alias to $hierarchy")
        hierarchy.withNewChildren(Subquery(alias, transform(hierarchy.child, ids)) :: Nil)
      case SingleQuery(select, from, where, groupBy, _ /* TODO */, orderBy, limit, distinct)
        if from != plan =>
        val fromPlan = transform(from, ids)
        val fromSubqueryPlan = if (getName(fromPlan).isEmpty && !fromPlan.isInstanceOf[Join]) {
          Subquery(newQueryName(ids), fromPlan)
        } else {
          fromPlan
        }
        val filterPlan = if (where.nonEmpty) {
          Filter(where.reduceLeft(And.apply), fromSubqueryPlan)
        } else {
          fromSubqueryPlan
        }
        val projectPlan = if (groupBy.nonEmpty) {
          Aggregate(groupBy, select, filterPlan)
        } else if (select.nonEmpty) {
          Project(select, filterPlan)
        } else {
          filterPlan
        }
        val distinctPlan = if (distinct) {
          Distinct(projectPlan)
        } else {
          projectPlan
        }
        val orderByPlan = if (orderBy.nonEmpty) {
          Sort(orderBy, global = true, distinctPlan)
        } else {
          distinctPlan
        }
        val limitPlan = if (limit.isDefined) {
          Limit(limit.get, orderByPlan)
        } else {
          orderByPlan
        }
        limitPlan
      case other if other.children.exists(isSetOperation) =>
        val newChildren = other.children map { child =>
          val alias = newQueryName(ids)
          logTrace(s"Added subquery $alias to $child")
          Subquery(alias, transform(child, ids))
        }
        other.withNewChildren(newChildren)
      case other =>
        val newChildren = other.children.map { child =>
          transform(child, ids)
        }
        other.withNewChildren(newChildren)
    }

}
