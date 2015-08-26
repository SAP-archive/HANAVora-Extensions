package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.{LogicalRelation, SqlLikeRelation}

/**
 * Change any Qualifier in a AttributeReference to the table name to avoid bad generated queries
 * like:
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
    var i: Int = 1
    val planWithSubqueries = plan transformUp {
      case relation@LogicalRelation(baseRelation: SqlLikeRelation) =>
        val newName = s"table$i"
        i += 1
        logTrace(s"Added subquery $newName to table ${baseRelation.tableName}")
        Subquery(newName, relation)
      case proj@Project(projectList, child) =>
        val newName = s"table$i"
        i += 1
        logTrace(s"Added subquery $newName to projection")
        Subquery(newName, proj)
      case agg@Aggregate(_, _, child) =>
        val newName = s"table$i"
        i += 1
        logTrace(s"Added subquery $newName to aggregation")
        Subquery(newName, agg)
      case f@Filter(_, child) =>
        val newName = s"table$i"
        i += 1
        logTrace(s"Added subquery $newName to filter")
        Subquery(newName, f)
      /* Put Limit and OrderBy below Subqueries */
      case l@Limit(n, s@Subquery(alias, child)) =>
        Subquery(alias, Limit(n, child))
      case l@Sort(so, b, Subquery(alias, child)) =>
        Subquery(alias, Sort(so, b, child))
      case other => other
    }
    val transformedPlan = planWithSubqueries transformUp {
      case lr: LogicalRelation => lr
      case Subquery(name, Subquery(innerName, child)) =>
        /* If multiple subqueries, preserve the outer one */
        logDebug(s"Nested subqueries ($name, $innerName) -> $name")
        Subquery(name, child)
      case lp: LogicalPlan with Product =>
        /* TODO
            val mo : LogicalPlan = lp match {
              case Join(l, r, jt, c) =>
                val newL = removeDummyPlans(l) match {
                  // the scope of projection alias is limited to subquery
                  // therefor we can use it outside as well.
                  case project@Project(p, Subquery(alias, q)) =>
                      Subquery(alias, project)
                  case other => other
                }
                val newR = removeDummyPlans(r) match {
                  // the scope of projection alias is limited to subquery
                  // therefor we can use it outside as well.
                  case project@Project(p, Subquery(alias, q)) =>
                    Subquery(alias, project)
                  case other => other
                }
                Join(newL, newR, jt, c)
              case agg@Aggregate(a, b, c@DummyPlan(d:Aggregate)) =>
                val newName = s"aggregate$i"
                i += 1
                Aggregate(a, b, Subquery(newName, c))
              case agg@Aggregate(a, b, c:Aggregate) =>
                val newName = s"aggregate$i"
                i += 1
                Aggregate(a, b, Subquery(newName, c))
              case a:LogicalPlan => a
            }
            */
        val expressionMap = lp.collect {
          case subquery@Subquery(alias, _) =>
            subquery.output.map({ attr => (attr.exprId, alias) })
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
                attr
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
