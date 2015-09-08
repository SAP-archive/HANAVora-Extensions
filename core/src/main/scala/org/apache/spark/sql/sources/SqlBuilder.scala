package org.apache.spark.sql.sources

import java.math.BigInteger
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.analysis.{AddSubqueries, ChangeQualifiersToTableNames}
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{analysis, expressions => expr}
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

  protected def formatAttributeWithQualifiers(qualifiers: Seq[String], name: String): String =
    (qualifiers :+ name).map({ s => s""""$s"""" }).mkString(".")

  protected def formatTableName(namespace: Option[String], tableName: String): String =
    formatAttributeWithQualifiers(namespace.toSeq, tableName)

  protected def formatRelation(relation: SqlLikeRelation): String =
    formatTableName(relation.nameSpace, relation.tableName)

  /**
   * Builds a SELECT query with optional WHERE and GROUP BY clauses.
   *
   * @param relation Table name, join clause or subquery for the FROM clause.
   * @param select List of fields for projection as strings.
   * @param where List of filters for the WHERE clause (can be empty).
   * @param groupBy List if expressions for the GROUP BY clause (can be empty).
   * @return A SQL string.
   */
  // scalastyle:off cyclomatic.complexity
  protected def buildQuery(relation: String,
                           select: Seq[String] = Nil,
                           where: Seq[String] = Nil,
                           groupBy: Seq[String] = Nil,
                           having: Option[String] = None,
                           orderBy: Seq[String] = Nil,
                           limit: Option[String] = None,
                           distinct: Boolean = false
                            ): String = {
    val selectStr = s"SELECT${if (distinct) " DISTINCT" else ""}"
    val selectColsStr = select match {
        // The optimizer sometimes does not report any fields (since no specifc is required by
        // the query (usually a nested select), thus we add the group by clauses as fields
      case Nil if groupBy.isEmpty =>  "*"
      case Nil => groupBy mkString ", "
      case s => s mkString ", "
    }
    val whereStr = where match {
      case Nil => ""
      case f => s" WHERE ${f mkString " AND "}"
    }
    val groupByStr = groupBy match {
      case Nil => ""
      case gb =>
        s" GROUP BY ${groupBy mkString ", "}"
    }
    val havingStr = having.map(h => s" HAVING $h").getOrElse("")
    val orderByStr = orderBy match {
      case Nil => ""
      case ob => s" ORDER BY ${ob mkString ", "}"
    }
    val limitStr = limit match {
      case None => ""
      case Some(l) => s" LIMIT $l"
    }
    s"$selectStr $selectColsStr FROM $relation$whereStr$groupByStr$havingStr$orderByStr$limitStr"
  }
  // scalastyle:on cyclomatic.complexity

  /**
   * Builds a SELECT query with optional WHERE and GROUP BY clauses.
   *
   * @param relation Table name, join clause or subquery for the FROM clause.
   * @param fields List of fields for projection as NamedExpression.
   * @param filters List of filters for the WHERE clause (can be empty).
   * @param groupByClauses List if expressions for the GROUP BY clause (can be empty).
   * @return A SQL string.
   */
  def buildSelect[F, H, G]
  (relation: SqlLikeRelation, fields: Seq[F], filters: Seq[H], groupByClauses: Seq[G])
  (implicit ev1: ToSql[F], ev2: ToSql[H], ev3: ToSql[G]): String = {
    buildQuery(
      formatRelation(relation),
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
  def buildSelect[F, H]
  (relation: SqlLikeRelation, fields: Seq[F], filters: Seq[H])
  (implicit ev1: ToSql[F], ev2: ToSql[H]): String = {
    buildQuery(
      formatRelation(relation),
      fields map ev1.toSql,
      filters map ev2.toSql,
      Nil
    )
  }

  /**
   * These rules prepare a logical plan to be convertible to a SQL query.
   * @return
   */
  protected def preparePlanRules: Seq[Rule[logical.LogicalPlan]] = Seq(
    AddSubqueries,
    ChangeQualifiersToTableNames
  )

  /**
   * Translates a logical plan to a SQL query string. It does not perform
   * any compatibility checks and assumes the plan is compatible as is.
   *
   * @param plan
   * @return
   */
  def logicalPlanToSql(plan: logical.LogicalPlan): String = {
    val preparedPlan = preparePlanRules.foldLeft(plan) {
      (processedPlan, rule) => rule(processedPlan)
    }
    internalLogicalPlanToSql(preparedPlan, noProject = true)
  }

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  protected[sources] def internalLogicalPlanToSql(
                                          plan: logical.LogicalPlan,
                                          noProject: Boolean = true): String =
    plan match {

      /*
       * A relation is converted to a query if the context does not allow a table name.
       * E.g. if the logical plan consists only of a relation, or as the only member inside
       *      a subquery.
       */
      case src.LogicalRelation(base: SqlLikeRelation) if noProject =>
        buildQuery(formatRelation(base), plan.output.map(expressionToSql))
      case src.LogicalRelation(base: SqlLikeRelation) => formatRelation(base)
      case analysis.UnresolvedRelation(name :: Nil, aliasOpt) => aliasOpt.getOrElse(name)
      case _: src.LogicalRelation =>
        sys.error("Cannot convert LogicalRelations to SQL unless they contain a SqlLikeRelation")

      case logical.Distinct(logical.Union(left, right)) =>
        s"""(${logicalPlanToSql(left)}) UNION (${logicalPlanToSql(right)})"""
      case logical.Union(left, right) =>
        s"""(${logicalPlanToSql(left)}) UNION ALL (${logicalPlanToSql(right)})"""
      case logical.Intersect(left, right) =>
        s"""(${logicalPlanToSql(left)}) INTERSECT (${logicalPlanToSql(right)})"""
      case logical.Except(left, right) =>
        s"""(${logicalPlanToSql(left)}) EXCEPT (${logicalPlanToSql(right)})"""

      case src.SingleQuery(select, from, where, groupBy, having, orderBy, limit, distinct)
        if plan != from =>
        if (!noProject) {
          sys.error("A full query without a subquery is not allowed in this context")
        }
        buildQuery(
          relation = internalLogicalPlanToSql(from, noProject = false),
          select = select map expressionToSql,
          where = where map expressionToSql,
          groupBy map expressionToSql,
          having = having map expressionToSql,
          orderBy = orderBy map expressionToSql,
          limit = limit map expressionToSql,
          distinct = distinct
        )

      case logical.Subquery(alias, src.LogicalRelation(relation: SqlLikeRelation)) if noProject =>
        val generatedQuery = buildQuery(
          formatRelation(relation),
          plan.output.map(expressionToSql))
        s"""$generatedQuery AS "$alias""""
      case logical.Subquery(alias, src.LogicalRelation(relation: SqlLikeRelation)) =>
        s"""${formatRelation(relation)} AS "$alias""""
      case logical.Subquery(alias, child) =>
        s"""(${internalLogicalPlanToSql(child)}) AS "$alias""""

      case _:logical.Join if noProject =>
        sys.error(s"Join not allowed in this context")
      case join@logical.Join(left, right, joinType, conditionOpt) =>
        val leftSql = internalLogicalPlanToSql(left, noProject = false)
        val rightSql = internalLogicalPlanToSql(right, noProject = false)
        val condition = conditionOpt match {
          case None => ""
          case Some(cond) => s" ON ${expressionToSql(cond)}"
        }
        s"$leftSql ${joinTypeToSql(join)} $rightSql$condition"

      case _ =>
        sys.error("Unsupported logical plan: " + plan)
    }
  // scalastyle:on method.length
  // scalastyle:on cyclomatic.complexity

  protected def joinTypeToSql(join: logical.Join): String = join match {
    case logical.Join(_, _, Inner, None) => "CROSS JOIN"
    case logical.Join(_, _, Inner, _) => "INNER JOIN"
    case logical.Join(_, _, LeftOuter, _) => "LEFT OUTER JOIN"
    case logical.Join(_, _, RightOuter, _) => "RIGHT OUTER JOIN"
    case logical.Join(_, _, FullOuter, _) => "FULL OUTER JOIN"
    case logical.Join(_, _, LeftSemi, _) => "LEFT SEMI JOIN"
    case _ => sys.error(s"Unsupported join type: ${join.joinType}")
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

  def toUnderscoreUpper( str: String ): String = {
    var result: String = str(0).toUpper.toString
    for( i <- 1 until str.length) {
      if( str(i-1).isLower && str(i).isUpper ) {
        result += '_'
      }
      result += str(i).toUpper
    }
    result
  }

  def generalExpressionToSql( expression: expr.Expression): String = {
    val name = expression.getClass.getSimpleName
    val children = expression.children
    val childStr = children.map(expressionToSql).mkString(", ")
    s"${toUnderscoreUpper(name)}($childStr)"
  }

  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  def expressionToSql(expression: expr.Expression): String =
    expression match {
      case expr.And(left, right) => s"(${expressionToSql(left)} AND ${expressionToSql(right)})"
      case expr.Or(left, right) => s"(${expressionToSql(left)} OR ${expressionToSql(right)})"
      case be: expr.BinaryExpression =>
        s"(${expressionToSql(be.left)} ${be.symbol} " +
          s"${expressionToSql(be.right)})"
      case expr.UnaryMinus(child) => s"-(${expressionToSql(child)})"
      case expr.IsNull(child) => s"${expressionToSql(child)} IS NULL"
      case expr.IsNotNull(child) => s"${expressionToSql(child)} IS NOT NULL"
      case expr.SortOrder(child,direction) =>
        val sortDirection = if (direction == Ascending) "ASC" else "DESC"
        s"${expressionToSql(child)} $sortDirection"
      case expr.Literal(value, _) => literalToSql(value)
      case expr.Cast(child, dataType) =>
        s"CAST(${expressionToSql(child)} AS ${typeToSql(dataType)})"
      case expr.ToVarChar(child) => s"TO_VARCHAR(${expressionToSql(child)})"
      case expr.CountDistinct(children) => s"COUNT(DISTINCT ${expressionsToSql(children, ",")})"
      case expr.Remainder(child, div) => s"MOD(${expressionToSql(child)}," +
        s"${expressionToSql(div)})"
      case expr.Coalesce(children) => s"COALESCE(${expressionsToSql(children, ",")})"
      case expr.Extract(flag,date) => s"EXTRACT(${flagToSql(flag)} FROM " +
        s"${expressionToSql(date)})"
      case expr.CurDate() => s"CURRENT_DATE()"
      case expr.Substring(str, pos, len) =>
        s"SUBSTRING(${expressionToSql(str)}, $pos, $len)"
      case expr.Average(child) => s"AVG(${expressionToSql(child)})"
      case expr.In(value, list) =>
        s"${expressionToSql(value)} IN (${list.map(expressionToSql).mkString(", ")})"
      case expr.InSet(value, hset) =>
        s"${expressionToSql(value)} IN (${hset.map(literalToSql).mkString(", ")})"
      case a@expr.Alias(child, name) =>
        s"""${expressionToSql(child)} AS "$name""""
      case a@expr.AttributeReference(name, _, _, _) =>
        formatAttributeWithQualifiers(a.qualifiers, name)
      case analysis.UnresolvedAttribute(name) =>
        formatAttributeWithQualifiers(name.reverse.tail.reverse, name.last)
      case a: analysis.Star => "*"

      case x =>
        generalExpressionToSql( x )
    }
  // scalastyle:on method.length
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

  // scalastyle:off cyclomatic.complexity
  protected def literalToSql(value: Any): String = value match {
    case s: String => s"'$s'"
    case s: UTF8String => s"'$s'"
    case i: Int    => s"$i"
    case l: Long    => s"$l"
    case f: Float    => s"$f"
    case d: Double    => s"$d"
    case b: Boolean    => s"$b"
    case bi: BigInteger    => s"$bi"
    case t: Timestamp => s"TO_TIMESTAMP('$t')"
    case d: Date => s"TO_DATE('$d')"
    case null => "NULL"
    case other => other.toString
  }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
  def typeToSql(sparkType: DataType): String =
    sparkType match {
      case `StringType` => "VARCHAR(*)"
      case `IntegerType` => "INTEGER"
      case `LongType` => "BIGINT"
      case `FloatType` => "FLOAT"
      case `DoubleType` => "DOUBLE"
      case DecimalType.Fixed(precision, scale) => s"DECIMAL($precision,$scale)"
      case `DateType` => "DATE"
      case `BooleanType` => "BOOLEAN"
      case `TimestampType` => "TIMESTAMP"
      case _ =>
        throw new IllegalArgumentException(s"Type $sparkType cannot be converted to SQL type")
    }
  // scalastyle:on cyclomatic.complexity

  protected def flagToSql(flag: expr.Expression): String = {
    flag.toString
  }

}

trait ToSql[T] {
  def toSql(t: T): String
}
