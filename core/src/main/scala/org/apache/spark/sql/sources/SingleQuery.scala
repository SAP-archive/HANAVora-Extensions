package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical

/**
 * Extractor for basic SQL queries (not counting subqueries).
 *
 * The output type is a tuple with the values corresponding to
 * SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT and
 * DISTINCT.
 *
 * We inspect the given [[logical.LogicalPlan]] top-down and stop
 * at any point where a subquery would be introduced or if nodes
 * need any re-ordering.
 *
 * The expected order of nodes is:
 *  - Limit
 *  - Sort
 *  - Distinct
 *  - Project / Aggregate
 *  - Filter
 *  - Any logical plan as the source relation.
 */
object SingleQuery extends PredicateHelper {

  type ReturnType = (
    Seq[NamedExpression], /* SELECT */
    logical.LogicalPlan,  /* FROM */
    Seq[Expression],      /* WHERE */
    Seq[Expression],      /* GROUP BY */
    Option[Expression],   /* HAVING */
    Seq[SortOrder],       /* ORDER BY */
    Option[Expression],   /* LIMIT */
    Boolean               /* DISTINCT */
    )

  private val initial: ReturnType = (Nil, null, Nil, Nil, None, Nil, None, false)

  def unapply(plan: logical.LogicalPlan): Option[ReturnType] = unapplyImpl(plan)

  // scalastyle:off cyclomatic.complexity
  private def unapplyImpl(plan: logical.LogicalPlan,
                          state: String = "LIMIT"): Option[ReturnType] =
    (state, plan) match {
      case ("RELATION", _) =>
        Some(initial.copy(_1 = plan.output, _2 = plan))
      case (_, _: logical.Subquery) =>
        unapplyImpl(plan, "RELATION")
      case ("LIMIT", logical.Limit(limit, child)) =>
        unapplyImpl(child, "ORDER BY").map(_.copy(_7 = Some(limit)))
      case ("LIMIT", _) =>
        unapplyImpl(plan, "ORDER BY")
      case ("ORDER BY", logical.Sort(sortOrder, global, child)) =>
        unapplyImpl(child, "SELECT").map(_.copy(_6 = sortOrder))
      case ("ORDER BY", _) =>
        unapplyImpl(plan, "SELECT")
      case ("ORDER BY" | "LIMIT" | "SELECT", logical.Distinct(child)) =>
        unapplyImpl(child, state).map(_.copy(_8 = true))
      case ("SELECT", logical.Project(select, child)) =>
        unapplyImpl(child, "WHERE").map(_.copy(_1 = select))
      case ("SELECT", _) =>
        unapplyImpl(plan, "GROUP BY")
      case ("GROUP BY", logical.Aggregate(ge, ae, child)) =>
        unapplyImpl(child, "WHERE").map(_.copy(_1 = ae, _4 = ge))
      case ("GROUP BY", _) =>
        unapplyImpl(plan, "WHERE")
      case ("WHERE", logical.Filter(where, child)) =>
        unapplyImpl(child, "RELATION").map(_.copy(_3 = where :: Nil))
      case ("WHERE", _) =>
        unapplyImpl(plan, "RELATION")
    }
  // scalastyle:on cyclomatic.complexity

}
