package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.{LogicalRelation, SqlLikeRelation}

/**
 * Fix qualifiers in the logical plan. This requires appropiate
 * subqueries in the plan. See [[AddSubqueries]].
 *
 * Original query: SELECT name FROM table1 as t1
 * Velocity generated query: SELECT "t1"."name" FROM "table1"
 *
 */
object ChangeQualifiersToTableNames extends Rule[LogicalPlan] {

  /**
   * XXX: This prefix is used to force to method "transformExpressionsDown"
   *      to trait the new copy of AttributeReference as a different one,
   *      because the "equals" method only check the exprId, name and the dataType.
   * See:
   * https://issues.apache.org/jira/browse/SPARK-8658
   */
  private val PREFIX = "XXX___"

  /**
   * XXX: Used to force change on transformUp (SPARK-8658)
   */
  private case class DummyPlan(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
  }

  private def removeDummyPlans(p: LogicalPlan): LogicalPlan =
    p transformUp {
      /* TODO: This is duplicated here */
      case Subquery(name, Subquery(innerName, child)) =>
        /* If multiple subqueries, preserve the outer one */
        logDebug(s"Nested subqueries ($name, $innerName) -> $name")
        Subquery(name, child)
      case DummyPlan(child) => child
      case p => p
    }

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val transformedPlan = plan transformUp {
      case lr: LogicalRelation => lr
      case lp: LogicalPlan with Product =>

        val expressionMap = lp.collect {
          case subquery@Subquery(alias, _) =>
            subquery.output.map({ attr => (attr.exprId, alias) })
          case lr@LogicalRelation(r: SqlLikeRelation) =>
            lr.output.map({ attr => (attr.exprId, r.tableName) })
        }.reverse.flatten.toMap

        val prefixedAttributeReferencesPlan = lp transformExpressionsDown {
          case attr: AttributeReference if attr.qualifiers.length > 1 =>
            sys.error(s"Only 1 qualifier is supported per attribute: $attr ${attr.qualifiers}")
          case attr: AttributeReference =>
            expressionMap.get(attr.exprId) match {
              case Some(q) =>
                logTrace(s"Using new qualifier ($q) for attribute: $attr")
                attr.copy(name = PREFIX.concat(attr.name))(
                  exprId = attr.exprId, qualifiers = q :: Nil
                )
              case None =>
                logWarning(s"Qualifier not found for expression ID: ${attr.exprId}")
                attr.copy(name = PREFIX.concat(attr.name))(
                  exprId = attr.exprId, Nil
                )
            }
        }
        /* Now we need to delete the prefix in all the attributes. */
        DummyPlan(prefixedAttributeReferencesPlan transformExpressionsDown {
          case attr: AttributeReference =>
            attr.copy(name = attr.name.replaceFirst(PREFIX, ""))(
              exprId = attr.exprId, qualifiers = attr.qualifiers
            )
        })
    }
    removeDummyPlans(transformedPlan) match {
      /* Remove outer subquery, if any */
      case Subquery(_, child) if !child.isInstanceOf[LogicalRelation] => child
      case other => other
    }
  }

}
