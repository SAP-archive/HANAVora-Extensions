package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
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
   * This prefix is used to force to method "transformExpressionsDown"
   * to trait the new copy of AttributeReference as a different one,
   * because the "equals" method only check the exprId, name and the dataType.
   */
  private val PREFIX = "XXX___"

  // scalastyle:off cyclomatic.complexity
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _: LogicalRelation => plan
      case _ =>
        var i: Int = 1
        plan transformUp {
          case relation@LogicalRelation(baseRelation: SqlLikeRelation) =>
            val newName = s"table$i"
            i += 1
            Subquery(newName, relation)
          case lp: LogicalPlan with Product =>
            val expressionMap = lp.collect {
              case subquery@Subquery(alias, child) =>
                subquery.output.map({ attr => (attr.exprId, alias) })
            }.flatten.toMap
            val prefixedAttributeReferencesPlan = lp transformExpressionsDown {
              case attr: AttributeReference if attr.qualifiers.length > 1 =>
                sys.error(s"Qualifiers of $attr will be only one, but was ${attr.qualifiers}")
              case attr: AttributeReference if !expressionMap.contains(attr.exprId) =>
                sys.error(s"No relation contains the expression id ${attr.exprId}")
              case attr: AttributeReference =>
                val newQualifier = expressionMap(attr.exprId)
                attr.copy(name = PREFIX.concat(attr.name))(
                  exprId = attr.exprId, qualifiers = newQualifier :: Nil
                )
            }
            /* Now we need to delete the prefix in all the attributes. */
            prefixedAttributeReferencesPlan transformExpressionsDown {
              case attr: AttributeReference =>
                attr.copy(name = attr.name.replaceFirst(PREFIX, ""))(
                  exprId = attr.exprId, qualifiers = attr.qualifiers
                )
            }
        }
    }
  }
}
