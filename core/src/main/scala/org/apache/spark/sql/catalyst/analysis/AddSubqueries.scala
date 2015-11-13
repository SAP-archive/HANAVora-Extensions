package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.IsLogicalRelation
import org.apache.spark.sql.sources.sql.{SingleQuery, SqlLikeRelation}

/**
  * Adds subqueries where needed for SQL generation. This rule is used
  * by [[org.apache.spark.sql.sources.CatalystSource]] implementations
  * that need to generate a SQL query from a [[LogicalPlan]]. Adding
  * subqueries eases to to create 1-to-1 mappings between [[LogicalPlan]]
  * and SQL.
  *
  * For example:
  *   Project(columns1, Project(columns2, myRelation))
  * is translated to:
  *   Project(columns1, Subquery("\_\_subquery1", Project(columns2, myRelation)))
  * which eases the generation of the following SQL query:
  *   SELECT columns1 FROM (SELECT columns2 FROM myRelation) AS "\_\_subquery1";
  *
  * Subqueries are introduced between any two [[LogicalPlan]] nodes where a direct
  * SQL mapping is not possible.
  *
  * TODO: This rule might be moved to [[org.apache.spark.sql.sources.sql]].
  *
  * @see [[org.apache.spark.sql.sources.sql.SqlBuilder]]
  */
object AddSubqueries extends Rule[LogicalPlan] {

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val planWithSubqueries = addSubqueries(plan)
    removeRedundantSubqueries(planWithSubqueries)
  }

  /**
    * Main logic to add subqueries.
    *
    * @param plan Input [[LogicalPlan]].
    * @param ids An ID generator, defaults to a fresh one.
    * @return [[LogicalPlan]] with subqueries.
    */
  private[this] def addSubqueries(plan: LogicalPlan, ids: Iterator[Int] = newIds()): LogicalPlan =
    plan match {

      /** End recursion at leaves */
      case relation@IsLogicalRelation(_: SqlLikeRelation) =>
        relation

      /** Subqueries are preserved as they are */
      case Subquery(alias, child) =>
        Subquery(alias, addSubqueries(child, ids))

      /**
        * Distinct(Union(left, right)) is equivalent to SQL UNION [DISTINCT].
        * (not UNION ALL). So we avoid introducing subqueries between Distinct
        * and Union nodes. Otherwise, it would lead to
        * SELECT DISTINCT * FROM (left UNION ALL right);
        */
      case Distinct(union@Union(left, right)) =>
        Distinct(addSubqueries(union))

      /**
        * On JOIN, we check left and right side:
        *   - If a side has no name, we add a subquery.
        *   - If a side has the same name as the other side, we add subquery.
        *     This prevents conflicts on self-joins.
        */
      case Join(left, right, joinType, condition) =>
        val leftName = getName(left)
        val rightName = getName(right)
        val newLeft = if (leftName.isEmpty || rightName == leftName) {
          Subquery(newQueryName(ids), addSubqueries(left, ids))
        } else {
          addSubqueries(left, ids)
        }
        val newRight = if (rightName.isEmpty || rightName == leftName) {
          Subquery(newQueryName(ids), addSubqueries(right, ids))
        } else {
          addSubqueries(right, ids)
        }
        Join(newLeft, newRight, joinType, condition)

      /**
        * If the plan is a hierarchy, we always add a subquery to the child.
        */
      case hierarchy: Hierarchy =>
        val alias = newQueryName(ids)
        logTrace(s"Added subquery $alias to $hierarchy")
        hierarchy.withNewChildren(Subquery(alias, addSubqueries(hierarchy.child, ids)) :: Nil)

      /**
        * Use [[SingleQuery]] to get a subtree of the plan that can be represented
        * as a SQL query without subqueries, set operations or joins. Then preserve it
        * and add a subquery after its FROM clause.
        */
      case SingleQuery(select, from, where, groupBy, _ /* TODO */, orderBy, limit, distinct)
        if from != plan =>
        val fromPlan = addSubqueries(from, ids)
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

      /**
        * If children of the current node contain a set operation, we
        * add subqueries to every children.
        */
      case other if other.children.exists(isSetOperation) =>
        val newChildren = other.children map { child =>
          val alias = newQueryName(ids)
          logTrace(s"Added subquery $alias to $child")
          Subquery(alias, addSubqueries(child, ids))
        }
        other.withNewChildren(newChildren)

      /**
        * In any case that was not covered, do not add a subquery and
        * continue the recursion with all children.
        */
      case other =>
        val newChildren = other.children.map { child =>
          addSubqueries(child, ids)
        }
        other.withNewChildren(newChildren)
    }

  /** Removes redundant subqueries */
  private[this] def removeRedundantSubqueries(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      case Subquery(name, Subquery(innerName, child)) =>
        /* If multiple subqueries, preserve the inner one */
        logDebug(s"Nested subqueries ($name, $innerName) -> $innerName")
        Subquery(innerName, child)
    } match {
      /* Avoid outer subquery */
      case Subquery(name, child) => child
      case other => other
    }

  /** Returns true if the input [[LogicalPlan]] is a set operation */
  private[this] def isSetOperation(lp: LogicalPlan): Boolean = lp match {
    case _: Union | _: Except | _: Intersect => true
    case _ => false
  }

  /**
    * Gets the name of a [[LogicalPlan]], if any. A name can be provided by:
    *   - Subquery name.
    *   - [[SqlLikeRelation]]
    *
    * @param plan Input [[LogicalPlan]].
    * @return Name, if any.
    */
  private[this] def getName(plan: LogicalPlan): Option[String] =
    plan match {
      case IsLogicalRelation(table: SqlLikeRelation) => Some(table.tableName)
      case Subquery(alias, child) => Some(alias)
      case _ => None
    }

  /** Creates an iterator of autoincremental integer IDs. */
  private def newIds(): Iterator[Int] = Stream.from(1).iterator

  /** Returns a new subquery alias based on an ID generator. */
  private[this] def newQueryName(ids: Iterator[Int]): String = s"__subquery${ids.next()}"

}
