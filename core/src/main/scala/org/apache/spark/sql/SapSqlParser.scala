package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.tablefunctions.UnresolvedTableFunction
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{CreateNonPersistentViewCommand, SapDDLParser}
import org.apache.spark.sql.sources.commands.{DescribeQueryCommand, DescribeRelationCommand}
import org.apache.spark.sql.sources.sql.{Cube, Dimension, Plain, ViewKind}

import scala.util.parsing.input.Position

/**
 * SQL parser based on [[org.apache.spark.sql.catalyst.SqlParser]] with
 * extended syntax and fixes.
 *
 * This parser covers only SELECT and CREATE [TEMPORARY] VIEW statements.
 * For DML statements see [[SapDDLParser]].
 */
private object SapSqlParser extends BackportedSqlParser
with AnnotationParsingRules{

  /* Hierarchies keywords */
  protected val HIERARCHY = Keyword("HIERARCHY")
  protected val USING = Keyword("USING")
  protected val PARENT = Keyword("PARENT")
  protected val SEARCH = Keyword("SEARCH")
  protected val START = Keyword("START")
  protected val SET = Keyword("SET")

  /* Describe table keyword */
  protected val OLAP_DESCRIBE = Keyword("OLAP_DESCRIBE")

  /* Views keywords */
  protected val CREATE = Keyword("CREATE")
  protected val VIEW = Keyword("VIEW")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val DIMENSION = Keyword("DIMENSION")
  protected val CUBE = Keyword("CUBE")

  /* Extract keywords */
  protected val EXTRACT = Keyword("EXTRACT")

  lexical.delimiters += "$"

  protected lazy val viewKind: Parser[String] = DIMENSION | CUBE

  /**
   * This is copied from [[projection]] but extended to allow annotations
   * on the attributes.
   */
  override protected lazy val projection: Parser[Expression] =
    (expression ~ (AS ~> ident) ~ metadata ^^ {
      case e ~ a ~ k => AnnotatedAttribute(Alias(e, a)())(k)
    }
    | expression ~ metadataFilter ^^ {
      case e ~ f => AnnotationFilter(e)(f)
    }
    | rep1sep(ident, ".") ~ metadata ^^ {
      case e ~ k =>
        AnnotatedAttribute(Alias(UnresolvedAttribute(e), e.last)())(k)
    }
    | expression ~ (AS.? ~> ident.?) ^^ {
      case e ~ a => a.fold(e)(Alias(e, _)())
    }
    )

  /**
   * This is the starting rule from, where parsing always starts.
   *
   * Overriden to hook [[createView]] parser.
   */
  override protected lazy val start: Parser[LogicalPlan] =
    start1 | insert | cte | createView | describeTable

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

  /** Create temporary [dimension] view parser. */
  protected lazy val createView: Parser[LogicalPlan] =
    (CREATE ~> TEMPORARY.?) ~ (viewKind.? <~ VIEW) ~ (ident <~ AS) ~ start1 ^^ {
      case temp ~ ViewKind(kind) ~ name ~ query =>
        val view = kind match {
        case Dimension =>
          NonPersistedDimensionView(query)
        case Cube =>
          NonPersistedCubeView(query)
        case Plain =>
          NonPersistedView(query)
        }
        CreateNonPersistentViewCommand(view, TableIdentifier(name), temp.isDefined)
    }

  protected lazy val describeTable: Parser[LogicalPlan] =
    (OLAP_DESCRIBE ~> (ident <~ ".").? ~ ident ^^ {
      case db ~ tbl =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            Seq(dbName, tbl)
          case None =>
            Seq(tbl)
        }
        DescribeRelationCommand(UnresolvedRelation(tblIdentifier, None))
    }
    |OLAP_DESCRIBE ~> start1 ^^ { l:LogicalPlan => DescribeQueryCommand(l) })

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

  override protected lazy val relationFactor: Parser[LogicalPlan] =
    ident ~ ("(" ~> repsep(start1, ",") <~ ")") ^^ {
      case name ~ arguments =>
        UnresolvedTableFunction(name, arguments)
    } |
    ( rep1sep(ident, ".") ~ (opt(AS) ~> opt(ident)) ^^ {
      case tableIdent ~ alias => UnresolvedRelation(tableIdent, alias)
    } |
    ("(" ~> start <~ ")") ~ (AS.? ~> ident) ^^ { case s ~ a => Subquery(a, s) })

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
