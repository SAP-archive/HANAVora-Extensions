package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Collapses an [[Expand]] plan into [[Project]]s and [[Subquery]]s, if possible.
  *
  * For instance an `Expand(Seq(Seq(a, 1), Seq(b, 1), Relation(a, b))` where 'a' and 'b' are
  * columns of the relation would be transformed into a `Project(a, 1, b, 1, Relation(a, b))`.
  */
object CollapseExpand extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case SqlConvertibleExpand(projectionExpressions, output, child) =>
      /**
        * All the projections are zipped together with their corresponding output
        * [[Attribute]]. In the case of a regular [[NamedExpression]], we just use the
        * output [[Attribute]] for the resulting projection. In case of a [[Literal]], we
        * create an [[Alias]] and wrap the [[Literal]] in it and naming it with the
        * output [[Attribute]] name.
        */
      val projectList = projectionExpressions.flatten.zip(output).map {
        case (l: Literal, correspondingOutput) => Alias(l, correspondingOutput.name)()
        case (_: NamedExpression, correspondingOutput) => correspondingOutput
      }
      Project(projectList, child)
  }
}

/**
  * Checks if an [[Expand]] can be converted to sql via its `unapply` method.
  */
object SqlConvertibleExpand {
  /**
    * Extracts projections, output and the child of an [[Expand]] if it is SQL-convertible.
    *
    * Expands are considered SQL-convertible if all its projections are sql convertible. A
    * projection is considered SQL-convertible if it is either a named expression
    * (thus it can be referenced in a SQL statement like `SELECT <name> FROM ...` or if it is a
    * [[Literal]] (like `1`), since these can also be included in a SELECT statement like
    * `SELECT 1 FROM ...`.
    *
    * @param expand The [[Expand]] to check if it is SQL-convertible and extract from.
    * @return [[Some]]([[Seq]][[Expression]], [[Seq]][[Attribute]], [[LogicalPlan]]) if the
    *         [[Expand]] is SQL-convertible, else [[None]]
    */
  def unapply(expand: Expand): Option[(Seq[Seq[Expression]], Seq[Attribute], LogicalPlan)] =
    if (expand.projections.forall(_.forall(isSqlConvertibleExpression))) {
      Some(expand.projections, expand.output, expand.child)
    } else {
      None
    }

  /**
    * Checks if a given [[Expression]] of an [[Expand]] can be converted to SQL.
    *
    * This is only true iff the given [[Expression]] is either a [[NamedExpression]]
    * or a [[Literal]].
    *
    * @param expression The [[Expression]] to check.
    * @return `true` iff the [[Expression]] is a [[NamedExpression]] or a [[Literal]],
    *         `false` otherwise.
    */
  private def isSqlConvertibleExpression(expression: Expression): Boolean =
    expression.isInstanceOf[NamedExpression] || expression.isInstanceOf[Literal]
}
