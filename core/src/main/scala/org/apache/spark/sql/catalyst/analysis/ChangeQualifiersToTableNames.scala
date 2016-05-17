package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.IsLogicalRelation
import org.apache.spark.sql.sources.sql.SqlLikeRelation

/**
  * Fix qualifiers in the [[LogicalPlan]] according to SQL rules.
  * This requires appropriate subqueries in the plan. See [[AddSubqueries]].
  *
  * TODO: This might be moved to [[org.apache.spark.sql.sources.sql]].
  */
object ChangeQualifiersToTableNames extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      case lr@IsLogicalRelation(_) => lr
      case lp: LogicalPlan with Product =>

        /** Get a mapping from [[ExprId]] to qualifier */
        val expressionIdMap = collectExpressionIdMap(lp)

        /** Add qualifiers to every attribute reference based on [[ExprId]] */
        lp transformExpressionsDown {
          case attr: AttributeReference if attr.qualifiers.length > 1 =>
            sys.error(s"Only 1 qualifier is supported per attribute: $attr ${attr.qualifiers}")
          case attr: AttributeReference =>
            expressionIdMap.get(attr.exprId) match {
              case Some(q) =>
                logTrace(s"Using new qualifier ($q) for attribute: $attr")
                withQualifiers(attr, q :: Nil)
              case None =>
                logWarning(s"Qualifier not found for expression ID: ${attr.exprId}")
                withQualifiers(attr, Nil)
            }
        }
    }
  }

  /**
    * Collects a [[Map(ExprId,String)]] where, for each [[ExprId]] we have
    * the name of the table/subquery that we should use to reference the attribute
    * at the current level.
    *
    * @param plan Input [[LogicalPlan]].
    * @return The [[ExprId]] -> [[String]] map.
    */
  private[this] def collectExpressionIdMap(plan: LogicalPlan): Map[ExprId, String] =
    plan.collect({

      /** If the node is a [[Subquery]], use the subquery name. */
      case subquery@Subquery(alias, _) =>
        subquery.output.map({ attr => (attr.exprId, alias) })

      /** If the node is a [[SqlLikeRelation]], use its table name. */
      case lr@IsLogicalRelation(r: SqlLikeRelation) =>
        lr.output.map({ attr => (attr.exprId, r.tableName) })

      /**
        * If the node is a [[AdjacencyListHierarchySpec]], use child alias name for attributes
        * whose [[ExprId]] is not found in the child relation. This is a workaround
        * to the fact that the parenthood expression of a hierarchy references the
        * child relation with two different qualifiers depending on its role. See
        * the hierarchies `JOIN PARENT` syntax to know more about this.
        */
      case spec:AdjacencyListHierarchySpec =>
        spec.parenthoodExp.references flatMap {
          case a: Attribute if spec.child.output.exists(_.exprId == a.exprId) => None
          case a =>
            Some(a.exprId -> spec.childAlias)
        }

    }).reverse.flatten.toMap

  /**
    * Adds a qualifier to an attribute.
    *
    * @param attr An [[AttributeReference]].
    * @param qualifiers New qualifiers.
    * @return New [[AttributeReference]] with new qualifiers.
    */
  private[this] def withQualifiers(
    attr: AttributeReference,
    qualifiers: Seq[String]): AttributeReference =
    attr.copy()(exprId = attr.exprId, qualifiers = qualifiers)
}
