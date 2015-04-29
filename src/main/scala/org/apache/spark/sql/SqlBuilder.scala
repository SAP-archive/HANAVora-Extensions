package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.{expressions => expr}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{sources => src}

/**
 * Helper methods to build SQL queries.
 */
 abstract class SqlBuilder {

  /**
   * Builds a SELECT query with optional WHERE and GROUP BY clauses.
   *
   * @param table Name of the FROM table.
   * @param fields List of fields for projection as strings.
   * @param filters List of filters for the WHERE clause (can be empty).
   * @param groupByClauses List if expressions for the GROUP BY clause (can be empty).
   * @return A SQL string.
   */
   def buildQuery(table : String, fields : Seq[String],
                             filters : Seq[String],
                             groupByClauses : Seq[String]) : String = {
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
    s"""SELECT $fieldList FROM "$table"$where$groupBy"""
  }

  /**
   * Builds a SELECT query with optional WHERE and GROUP BY clauses.
   *
   * @param table Name of the FROM table.
   * @param fields List of fields for projection as NamedExpression.
   * @param filters List of filters for the WHERE clause (can be empty).
   * @param groupByClauses List if expressions for the GROUP BY clause (can be empty).
   * @return A SQL string.
   */
  def selectFromWhereGroupBy(table: String, fields: Seq[NamedExpression],
                             filters: Seq[src.Filter],
                             groupByClauses: Seq[expr.Expression]): String =
    buildQuery(
      table,
      fields map (expressionToSql(_)),
      filters map filterToSql,
      groupByClauses map (expressionToSql(_))
    )

  /**
   * Builds a SELECT query with optional WHERE and GROUP BY clauses.
   *
   * @param table Name of the FROM table.
   * @param fields List of fields for projection.
   * @param filters List of filters for the WHERE clause (can be empty).
   * @return A SQL string.
   */
  def selectFromWhere(table: String, fields: Seq[String], filters: Seq[src.Filter]): String =
    buildQuery(table, fields map ("\"" + _ + "\""), filters map filterToSql, Nil)

  /**
   * Builds SELECT query with optional WHERE and Expressions
   *
   * @param table Name of the FROM table.
   * @param fields List of fields for projection.
   * @param filters List of filters for the WHERE clause (can be empty).
   * @return A SQL string.
   */
  def selectFromWhereExpressions(table: String,
                                 fields: Seq[expr.Expression],
                                 filters: Seq[expr.Expression]): String = {
    buildQuery(
      table,
      fields map (expressionToSql(_)),
      filters map (expressionToSql(_)),
      Nil
    )
  }

  // scalastyle:off cyclomatic.complexity
   def filterToSql(f: src.Filter): String =
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
      case x => throw new UnsupportedOperationException(s"Failed to parse filter: $x")
    }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
   def expressionToSql(expression: expr.Expression): String =
    expression match {
      case be: expr.BinaryExpression =>
        s"(${expressionToSql(be.left)} ${be.symbol} " +
          s"${expressionToSql(be.right)})"
      case expr.Literal(value, _) => literalToSql(value)
      case expr.Cast(child, TimestampType) => s"TO_TIMESTAMP(${expressionToSql(child)})"
      case expr.Cast(child, _) =>
        /* TODO: Velocity does not support CAST yet, so we ignore it */
        expressionToSql(child)
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
      case expr.Coalesce(children) => s"COALESCE(${expressionsToSql(children, "," )})"

      case a@expr.Alias(child, name) =>
        s"""${expressionToSql(child)} AS "$name""""

      case expr.AttributeReference(name, _, _, _) => s""""$name""""
      case x =>
        throw new UnsupportedOperationException(s"Could not convert to SQL: $x (${x.getClass}})")
    }
  // scalastyle:on cyclomatic.complexity

  /**
   * Convenience functions to take several expressions
   *
   * @param expressions
   * @param delimiter
   * @param useExprId
   * @return
   */
   def expressionsToSql(expressions: Seq[expr.Expression], delimiter: String = " ",
                               useExprId: Boolean = false): String = {
    expressions.map(expressionToSql(_)).reduceLeft((x, y) => x + delimiter + y)
  }

   def literalToSql(value: Any): String = value match {
    case s: String => s"'$s'"
    case t: Timestamp => s"TO_TIMESTAMP('$t')"
    case d: Date => s"TO_DATE('$d')"
    case null => "NULL"
    case other => other.toString
  }

}
