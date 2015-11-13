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
    val transformedPlan = plan transformUp {
      case lr@IsLogicalRelation(_) => lr
      case lp: LogicalPlan with Product =>

        /** Get a mapping from [[ExprId]] to qualifier */
        val expressionIdMap = collectExpressionIdMap(lp)

        /** Add qualifiers to every attribute reference based on [[ExprId]] */
        val planWithQualifiersFixed = lp transformExpressionsDown {
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

        /** Now we need to delete the prefix in all the attributes. SPARK-8658. See below. */
        DummyPlan(removeExpressionPrefixes(planWithQualifiersFixed))
    }

    /** Remove [[DummyPlan]] nodes. SPARK-8658. See below. */
    removeDummyPlans(transformedPlan)
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
        * If the node is a [[Hierarchy]], use child alias name for attributes that
        * whose [[ExprId]] is not found in the child relation. This is a workaround
        * to the fact that the parenthood expression of a hierarchy references the
        * child relation with two different qualifiers depending on its role. See
        * the hierarchies JOIN PARENT syntax to know more about this.
        */
      case h: Hierarchy =>
        h.parenthoodExpression.references flatMap {
          case a: Attribute if h.child.output.exists(_.exprId == a.exprId) =>
            None
          case a =>
            Some(a.exprId -> h.childAlias)
        }

    }).reverse.flatten.toMap

  //
  // Code to workaround SPARK-8658.
  // https://issues.apache.org/jira/browse/SPARK-8658
  // We need these tricks so that transformExpressionsDown
  // works when only qualifiers changed. Currently, Spark ignores
  // changes affecting only qualifiers.
  //

  /** XXX: Prefix to append temporarily (SPARK-8658) */
  private val PREFIX = "XXX___"

  /**
    * Adds a qualifier to an attribute.
    * Workarounds SPARK-8658.
    *
    * @param attr An [[AttributeReference]].
    * @param qualifiers New qualifiers.
    * @return New [[AttributeReference]] with new qualifiers.
    */
  private[this] def withQualifiers(
    attr: AttributeReference,
    qualifiers: Seq[String]): AttributeReference =
    attr.copy(name = PREFIX.concat(attr.name))(
      exprId = attr.exprId, qualifiers = qualifiers
    )

  /** XXX: Remove prefix from all expression names. SPARK-8658. */
  private[this] def removeExpressionPrefixes(plan: LogicalPlan): LogicalPlan =
    plan transformExpressionsDown {
      case attr: AttributeReference =>
        attr.copy(name = attr.name.replaceFirst(PREFIX, ""))(
          exprId = attr.exprId, qualifiers = attr.qualifiers
        )
    }

  /** XXX: Used to force change on transformUp (SPARK-8658) */
  private case class DummyPlan(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
  }

  /** XXX: Remove all [[DummyPlan]] (SPARK-8658) */
  private def removeDummyPlans(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      /* TODO: This is duplicated here */
      case Subquery(name, Subquery(innerName, child)) =>
        /* If multiple subqueries, preserve the outer one */
        logDebug(s"Nested subqueries ($name, $innerName) -> $name")
        Subquery(name, child)
      case DummyPlan(child) => child
      case p => p
    }

}
