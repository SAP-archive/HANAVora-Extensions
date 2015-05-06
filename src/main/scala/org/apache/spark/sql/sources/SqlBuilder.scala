package org.apache.spark.sql.sources

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.{analysis, expressions => expr, planning}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{sources => src}

/**
 * SQL builder class.
 */
class SqlBuilder {

  implicit object ExpressionToSql extends ToSql[expr.Expression] {
    override def toSql(e: expr.Expression): String = expressionToSql(e)
  }

  implicit object NamedExpressionToSql extends ToSql[expr.NamedExpression] {
    override def toSql(e: expr.NamedExpression): String = expressionToSql(e)
  }

  implicit object FilterToSql extends ToSql[src.Filter] {
    override def toSql(f: src.Filter): String = filterToSql(f)
  }

  implicit object StringToSql extends ToSql[String] {
    override def toSql(s: String): String = s""""$s""""
  }

  implicit object LogicalPlanToSql extends ToSql[logical.LogicalPlan] {
    override def toSql(p: logical.LogicalPlan): String =
      internalLogicalPlanToSql(p, noProject = false)
  }

  /**
   * Builds a SELECT query with optional WHERE and GROUP BY clauses.
   *
   * @param relation Table name, join clause or subquery for the FROM clause.
   * @param fields List of fields for projection as strings.
   * @param filters List of filters for the WHERE clause (can be empty).
   * @param groupByClauses List if expressions for the GROUP BY clause (can be empty).
   * @return A SQL string.
   */
  protected def buildQuery(relation: String, fields: Seq[String],
                           filters: Seq[String],
                           groupByClauses: Seq[String]): String = {
    val fieldList = fields match {
      case Nil => "*"
      case s => s mkString ", "
    }
    val where = filters match {
      case Nil => ""
      case f => s" WHERE ${f mkString " AND "}"
    }
    val groupBy = groupByClauses match {
      case Nil => ""
      case gb =>
        s" GROUP BY ${groupByClauses mkString ", "}"
    }
    s"""SELECT $fieldList FROM $relation$where$groupBy"""
  }

  /**
   * Builds a SELECT query with optional WHERE and GROUP BY clauses.
   *
   * @param relation Table name, join clause or subquery for the FROM clause.
   * @param fields List of fields for projection as NamedExpression.
   * @param filters List of filters for the WHERE clause (can be empty).
   * @param groupByClauses List if expressions for the GROUP BY clause (can be empty).
   * @return A SQL string.
   */
  def buildSelect[E, F, H, G]
  (relation: E, fields: Seq[F], filters: Seq[H], groupByClauses: Seq[G])
  (implicit ev0: ToSql[E], ev1: ToSql[F], ev2: ToSql[H], ev3: ToSql[G]): String = {
    buildQuery(
      ev0.toSql(relation),
      fields map ev1.toSql,
      filters map ev2.toSql,
      groupByClauses map ev3.toSql
    )
  }

  /**
   * Builds a SELECT query with optional WHERE clause.
   *
   * @param relation Table name, join clause or subquery for the FROM clause.
   * @param fields List of fields for projection as NamedExpression.
   * @param filters List of filters for the WHERE clause (can be empty).
   * @return A SQL string.
   */
  def buildSelect[E, F, H]
  (relation: E, fields: Seq[F], filters: Seq[H])
  (implicit ev0: ToSql[E], ev1: ToSql[F], ev2: ToSql[H]): String = {
    buildQuery(
      ev0.toSql(relation),
      fields map ev1.toSql,
      filters map ev2.toSql,
      Nil
    )
  }

  /**
   * Translates a logical plan to a SQL query string. It does not perform
   * any compatibility checks and assumes the plan is compatible as is.
   *
   * @param plan
   * @return
   */
  def logicalPlanToSql(plan: logical.LogicalPlan): String =
    internalLogicalPlanToSql(plan, noProject = true)

  // scalastyle:off cyclomatic.complexity
  protected def internalLogicalPlanToSql(
                                          plan: logical.LogicalPlan,
                                          noProject: Boolean = true): String =
    plan match {
      case src.LogicalRelation(base: SqlLikeRelation) if noProject =>
        s"""SELECT * FROM "${base.tableName}""""
      case src.LogicalRelation(base: SqlLikeRelation) => s""""${base.tableName}""""
      case analysis.UnresolvedRelation(name :: Nil, aliasOpt) => aliasOpt.getOrElse(name)
      case _: src.LogicalRelation =>
        sys.error("Cannot convert LogicalRelations to SQL unless they contain a SqlLikeRelation")
      case logical.Subquery(alias, child) => s"(${internalLogicalPlanToSql(child)}}) AS $alias"
      case logical.Join(left, right, joinType, conditionOpt) =>
        val condition = conditionOpt match {
          case None => ""
          case Some(cond) => s" ON ${expressionToSql(cond)}"
        }
        val leftSql = internalLogicalPlanToSql(left, noProject = false)
        val rightSql = internalLogicalPlanToSql(right, noProject = false)
        s"$leftSql ${joinTypeToSql(joinType)} $rightSql$condition"
      case p@planning.PhysicalOperation(fields, filters, child) if
      p.isInstanceOf[logical.Project] || p.isInstanceOf[logical.Filter] =>
        buildSelect(child, fields, filters)
      case logical.Aggregate(groupingExpressions, aggregateExpressions, child) =>
        buildSelect(
          child,
          fields = aggregateExpressions,
          filters = Seq[String](),
          groupByClauses = groupingExpressions
        )
      case logical.Limit(limitExpr, child) =>
        s"${internalLogicalPlanToSql(child)} LIMIT ${expressionToSql(limitExpr)}"
      case _ =>
        sys.error("Unsupported logical plan: " + plan)
    }

  // scalastyle:on cyclomatic.complexity

  protected def joinTypeToSql(joinType: JoinType): String = joinType match {
    case `Inner` => "INNER JOIN"
    case `LeftOuter` => "LEFT OUTER JOIN"
    case `RightOuter` => "RIGHT OUTER JOIN"
    case `FullOuter` => "FULL OUTER JOIN"
    case `LeftSemi` => "LEFT SEMI JOIN"
    case _ => sys.error(s"Unsupported join type: $joinType")
  }

  // scalastyle:off cyclomatic.complexity
  protected def filterToSql(f: src.Filter): String =
    f match {
      case src.EqualTo(name, value) => s""""$name" = ${literalToSql(value)}"""
      case src.GreaterThan(name, value) => s""""$name" > $value"""
      case src.GreaterThanOrEqual(name, value) => s""""$name" >= $value"""
      case src.LessThan(name, value) => s""""$name" < $value"""
      case src.LessThanOrEqual(name, value) => s""""$name" <= $value"""
      case src.In(name, values) => s""""$name" IN (${values map literalToSql mkString ","})"""
      case src.IsNull(name) => s""""$name" IS NULL"""
      case src.IsNotNull(name) => s""""$name" IS NOT NULL"""
      case src.And(left, right) => s"(${filterToSql(left)} AND ${filterToSql(right)})"
      case src.Or(left, right) => s"(${filterToSql(left)} OR ${filterToSql(right)})"
      case src.Not(child) => s"NOT(${filterToSql(child)})"
      case x => sys.error(s"Failed to parse filter: $x")
    }

  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
  def expressionToSql(expression: expr.Expression): String =
    expression match {
      case expr.And(left, right) => s"(${expressionToSql(left)} AND ${expressionToSql(right)})"
      case expr.Or(left, right) => s"(${expressionToSql(left)} OR ${expressionToSql(right)})"
      case be: expr.BinaryExpression =>
        s"(${expressionToSql(be.left)} ${be.symbol} " +
          s"${expressionToSql(be.right)})"
      case expr.SortOrder(child,direction) =>
        val sortDirection = if (direction == Ascending) "ASC" else "DESC"
        s"${expressionToSql(child)} $sortDirection"
      case expr.Literal(value, _) => literalToSql(value)
      case expr.Cast(child, dataType) => s"CAST($child AS ${typeToSql(dataType)}})"
      case expr.Sum(child) => s"SUM(${expressionToSql(child)})"
      case expr.Count(child) => s"COUNT(${expressionToSql(child)})"
      case expr.Average(child) => s"AVG(${expressionToSql(child)})"
      case expr.Min(child) => s"MIN(${expressionToSql(child)})"
      case expr.Max(child) => s"MAX(${expressionToSql(child)})"
      case expr.Substring(str, pos, len) =>
        s"SUBSTRING(${expressionToSql(str)}, $pos, $len)"
      case expr.Abs(child) => s"ABS(${expressionToSql(child)})"
      case expr.Lower(child) => s"LOWER(${expressionToSql(child)})"
      case expr.Upper(child) => s"UPPER(${expressionToSql(child)})"
      case expr.Not(child) => s"NOT(${expressionToSql(child)})"
      case expr.CountDistinct(children) => s"COUNT(DISTINCT ${expressionsToSql(children, ",")})"
      case expr.Coalesce(children) => s"COALESCE(${expressionsToSql(children, ",")})"
      case a@expr.Alias(child, name) =>
        s"""${expressionToSql(child)} AS "$name""""
      case a@expr.AttributeReference(name, _, _, _) =>
        (a.qualifiers :+ name).map(x => s""""$x"""").mkString(".")
      case analysis.UnresolvedAttribute(name) => s""""$name""""
      case a: analysis.Star => "*"
      case x =>
        sys.error(s"Could not convert to SQL: $x (${x.getClass}})")
    }

  // scalastyle:on cyclomatic.complexity

  /**
   * Convenience functions to take several expressions
   *
   * @param expressions
   * @param delimiter
   * @return
   */
  protected def expressionsToSql(expressions: Seq[expr.Expression],
                                 delimiter: String = " "): String = {
    expressions.map(expressionToSql).reduceLeft((x, y) => x + delimiter + y)
  }

  protected def literalToSql(value: Any): String = value match {
    case s: String => s"'$s'"
    case t: Timestamp => s"TO_TIMESTAMP('$t')"
    case d: Date => s"TO_DATE('$d')"
    case null => "NULL"
    case other => other.toString
  }

  def typeToSql(sparkType: DataType): String =
    sparkType match {
      case `StringType` => "VARCHAR(*)"
      case `IntegerType` => "INTEGER"
      case `LongType` => "BIGINT"
      case `DoubleType` => "DOUBLE"
      case DecimalType.Fixed(precision, scale) => s"DECIMAL($precision,$scale)"
      case `DateType` => "DATE"
      case `BooleanType` => "BOOLEAN"
      case `TimestampType` => "TIMESTAMP"
      case _ =>
        throw new IllegalArgumentException(s"Type $sparkType cannot be converted to SQL type")
    }

}

trait ToSql[T] {
  def toSql(t: T): String
}
