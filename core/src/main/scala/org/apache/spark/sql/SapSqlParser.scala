package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.compat.BackportedSqlParser
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan, Subquery}
import org.apache.spark.sql.execution.datasources.{CreateViewCommand, SapDDLParser}

import scala.util.parsing.input.Position

/**
 * SQL parser based on [[org.apache.spark.sql.catalyst.SqlParser]] with
 * extended syntax and fixes.
 *
 * This parser covers only SELECT and CREATE VIEW statements.
 * For DML statements see [[SapDDLParser]].
 */
private object SapSqlParser extends BackportedSqlParser {

  /* Hierarchies keywords */
  protected val HIERARCHY = Keyword("HIERARCHY")
  protected val USING = Keyword("USING")
  protected val PARENT = Keyword("PARENT")
  protected val SEARCH = Keyword("SEARCH")
  protected val START = Keyword("START")
  protected val SET = Keyword("SET")

  /* Views keywords */
  protected val CREATE = Keyword("CREATE")
  protected val VIEW = Keyword("VIEW")

  /* Extract keywords */
  protected val EXTRACT = Keyword("EXTRACT")

  lexical.delimiters += "$"

  /**
   * This is the starting rule from, where parsing always starts.
   *
   * Overriden to hook [[createView]] parser.
   */
  override protected lazy val start: Parser[LogicalPlan] =
    start1 | insert | cte | createView

  /**
   * Overriden to hook [[hierarchy]] parser.
   */
  override protected lazy val relation: Parser[LogicalPlan] =
    hierarchy | joinedRelation | relationFactor

  /**
    * Every function / expression parsing is hooked here.
    *
    * @note Do not add rules to parse new functions here unless
    *       they have special syntax. Functions with standard
    *       syntax should be registered with [[SQLContext.functionRegistry]].
    *       See [[RegisterCustomFunctions]].
    */
  override protected lazy val function: Parser[Expression] =
    extract | originalFunction | dataSourceFunctions

  /** Hierarchy parser. */
  protected lazy val hierarchy: Parser[LogicalPlan] =
    HIERARCHY ~> "(" ~>
      (USING ~> relationFactor) ~
      (JOIN ~> PARENT ~> ident) ~ (ON ~> expression) ~
      (SEARCH ~> BY ~> ordering).? ~
      (START ~> WHERE ~> expression).? ~
      (SET ~> ident <~ ")") ~
      (AS ~> ident) ^^ {
      case rel ~ ca ~ pexpr ~ sba ~ sw ~ nc ~ alias =>
        Subquery(alias, Hierarchy(
          relation = rel,
          childAlias = ca,
          parenthoodExpression = pexpr,
          searchBy = sba.getOrElse(Seq()),
          startWhere = sw,
          nodeAttribute = UnresolvedAttribute(nc)))
    }

  /** Create view parser. */
  protected lazy val createView: Parser[LogicalPlan] =
    (CREATE ~> VIEW ~> ident <~ AS) ~ start1 ^^ {
      case name ~ query => CreateViewCommand(name, query)
    }

  /** EXTRACT function. */
  protected lazy val extract: Parser[Expression] =
    EXTRACT ~ "(" ~> extractPart ~ (FROM ~> expression) <~ ")" ^^ { case f ~ d => f(d) }

  /** @see [[extract]] */
  protected lazy val extractPart: Parser[Expression => Expression] =
    (
      "(?i)DAY".r ^^^ { e: Expression => DayOfMonth(e) }
      | "(?i)MONTH".r ^^^ { e: Expression => Month(e) }
      | "(?i)YEAR".r ^^^ { e: Expression => Year(e) }
      | "(?i)HOUR".r ^^^  { e: Expression => Hour(e) }
      | "(?i)MINUTE".r ^^^ { e: Expression => Minute(e) }
      | "(?i)SECOND".r ^^^ { e: Expression => Second(e) }
      )

  /**
   * Parser for data source specific functions. That is, functions
   * prefixed with $, so that they are always push down to the data
   * source as they are.
   */
  protected lazy val dataSourceFunctions: Parser[Expression] =
    "$" ~> ident ~ ("(" ~> repsep(expression, ",") <~ ")") ^^
      { case udf ~ expr => DataSourceExpression(udf.toLowerCase, expr) }

  /*
   * TODO: Remove in Spark 1.4.1/1.5.0. This fixes NOT operator precendence, which we
   *       need for some SqlLogicTest queries.
   *       https://issues.apache.org/jira/browse/SPARK-6740
   */
  override protected lazy val andExpression: Parser[Expression] =
    booleanFactor * (AND ^^^ { (e1: Expression, e2: Expression) => And(e1, e2) })

  protected lazy val booleanFactor: Parser[Expression] =
    NOT.? ~ comparisonExpression ^^ {
      case notOpt ~ expr => notOpt.map(s => Not(expr)).getOrElse(expr)
    }

  override protected lazy val comparisonExpression: Parser[Expression] =
    (termExpression ~ ("="  ~> termExpression) ^^ { case e1 ~ e2 => EqualTo(e1, e2) }
      | termExpression ~ ("<"  ~> termExpression) ^^ { case e1 ~ e2 => LessThan(e1, e2) }
      | termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => LessThanOrEqual(e1, e2) }
      | termExpression ~ (">"  ~> termExpression) ^^ { case e1 ~ e2 => GreaterThan(e1, e2) }
      | termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => GreaterThanOrEqual(e1, e2) }
      | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
      | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
      | termExpression ~ ("<=>" ~> termExpression) ^^ { case e1 ~ e2 => EqualNullSafe(e1, e2) }
      | termExpression ~ NOT.? ~ (BETWEEN ~> termExpression) ~ (AND ~> termExpression) ^^ {
      case e ~ not ~ el ~ eu =>
        val betweenExpr: Expression = And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))
        not.fold(betweenExpr)(f => Not(betweenExpr))
    }
      | termExpression ~ (RLIKE  ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) }
      | termExpression ~ (REGEXP ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) }
      | termExpression ~ (LIKE   ~> termExpression) ^^ { case e1 ~ e2 => Like(e1, e2) }
      | termExpression ~ (NOT ~ LIKE ~> termExpression) ^^ { case e1 ~ e2 => Not(Like(e1, e2)) }
      | termExpression ~ (IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
      case e1 ~ e2 => In(e1, e2)
    }
      | termExpression ~ (NOT ~ IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
      case e1 ~ e2 => Not(In(e1, e2))
    }
      | termExpression <~ IS ~ NULL ^^ { case e => IsNull(e) }
      | termExpression <~ IS ~ NOT ~ NULL ^^ { case e => IsNotNull(e) }
      /* XXX: | NOT ~> termExpression ^^ {e => Not(e)} */
      | termExpression
      )


  /**
   * Main entry point for the parser.
   *
   * Overriden to control error handling.
   *
   * @param input Query to be parsed.
   * @return A [[LogicalPlan]].
   */
  override def parse(input: String): LogicalPlan = {
    /*
     * This is a workaround to a race condition in AbstractSparkSQLParser:
     * https://issues.apache.org/jira/browse/SPARK-8628
     */
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError =>
        // Now the native scala parser error is reformatted
        // to be non-misleading. An idea is to allow the user
        // to set the error message type in the future.
        val pos: Position = failureOrError.next.pos
        throw new SapParserException(input, pos.line, pos.column, failureOrError.toString)
    }
  }
}
