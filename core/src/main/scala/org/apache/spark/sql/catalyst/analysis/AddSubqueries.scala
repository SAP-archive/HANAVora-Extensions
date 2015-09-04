package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.{LogicalRelation, SqlLikeRelation}

/**
 * Adds subqueries where needed for SQL generation.
 */
object AddSubqueries extends Rule[LogicalPlan] {

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  override def apply(plan: LogicalPlan): LogicalPlan = {

    val ids = Stream.from(1).iterator

    @inline def newTableName: String = s"__table${ids.next()}"

    @inline def newQueryName: String = s"__subquery${ids.next()}"

    val planWithSubqueries = plan transformUp {

      case relation@LogicalRelation(baseRelation: SqlLikeRelation) =>
        val tableName = newTableName
        logTrace(s"Added subquery (alias) $tableName to table ${baseRelation.tableName}")
        Subquery(tableName, relation)

      case limit@Limit(_, _: Limit) =>
        val alias = newQueryName
        logTrace(s"Added subquery $alias to $limit")
        limit.withNewChildren(Subquery(alias, limit.child) :: Nil)

      case sort@Sort(_, _, _: Limit | _: Sort) =>
        val alias = newQueryName
        logTrace(s"Added subquery $alias to $sort")
        sort.withNewChildren(Subquery(alias, sort.child) :: Nil)

      case agg@Aggregate(_, _, _: Aggregate | _: Limit | _: Project | _: Sort) =>
        val alias = newQueryName
        logTrace(s"Added subquery $alias to $agg")
        agg.withNewChildren(Subquery(alias, agg.child) :: Nil)

      case proj@Project(_, _: Aggregate | _: Limit | _: Project | _: Sort) =>
        val alias = newQueryName
        logTrace(s"Added subquery $alias to $proj")
        proj.withNewChildren(Subquery(alias, proj.child) :: Nil)

      case filter@Filter(_, _: Aggregate | _: Filter | _: Limit | _: Project | _: Sort) =>
        val alias = newQueryName
        logTrace(s"Added subquery $alias to $filter")
        filter.withNewChildren(Subquery(alias, filter.child) :: Nil)

      case join: Join =>
        val newChildren = join.children map {
          case s: Subquery => s
          case child =>
            val alias = newQueryName
            logTrace(s"Added subquery $alias to $child")
            Subquery(alias, child)
        }
        join.withNewChildren(newChildren)

      case distinct@Distinct(_: Union) =>
        distinct

      case hierarchy: Hierarchy =>
        val alias = newQueryName
        logTrace(s"Added subquery $alias to $hierarchy")
        hierarchy.withNewChildren(Subquery(alias, hierarchy.child) :: Nil)

      case other if other.children.exists(isSetOperation) =>
        val newChildren = other.children map { child =>
          val alias = newQueryName
          logTrace(s"Added subquery $alias to $child")
          Subquery(alias, child)
        }
        other.withNewChildren(newChildren)

    }

    planWithSubqueries transformUp {
      case Subquery(name, Subquery(innerName, child)) =>
        /* If multiple subqueries, preserve the outer one */
        logDebug(s"Nested subqueries ($name, $innerName) -> $name")
        Subquery(name, child)
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

}
