package org.apache.spark.sql

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}
import org.apache.spark.sql.types.StringType

class HierarchiesSQLParser extends SqlParser {

  protected val HIERARCHY = Keyword("HIERARCHY")
  protected val USING = Keyword("USING")
  protected val PARENT = Keyword("PARENT")
  protected val SEARCH = Keyword("SEARCH")
  protected val START = Keyword("START")
  protected val SET = Keyword("SET")

  /* XXX Those expressions are not only for hierarchies */
  /* EXTRACT keywords */
  protected val EXTRACT = Keyword("EXTRACT")
  protected val DAY = Keyword("DAY")
  protected val MONTH = Keyword("MONTH")
  protected val YEAR = Keyword("YEAR")
  protected val HOUR = Keyword("HOUR")
  protected val MINUTE = Keyword("MINUTE")
  protected val SECOND = Keyword("SECOND")

  override protected lazy val relation: Parser[LogicalPlan] =
    hierarchy | joinedRelation | relationFactor

  override protected lazy val function: Parser[Expression] = extract | sparkFunctions

  // scalastyle:off
  /* TODO SparkSQL parser functions code copied */
  protected lazy val sparkFunctions: Parser[Expression] =
    ( SUM   ~> "(" ~> expression             <~ ")" ^^ { case exp => Sum(exp) }
      | SUM   ~> "(" ~> DISTINCT ~> expression <~ ")" ^^ { case exp => SumDistinct(exp) }
      | COUNT ~  "(" ~> "*"                    <~ ")" ^^ { case _ => Count(Literal(1)) }
      | COUNT ~  "(" ~> expression             <~ ")" ^^ { case exp => Count(exp) }
      | COUNT ~> "(" ~> DISTINCT ~> repsep(expression, ",") <~ ")" ^^
      { case exps => CountDistinct(exps) }
      | APPROXIMATE ~ COUNT ~ "(" ~ DISTINCT ~> expression <~ ")" ^^
      { case exp => ApproxCountDistinct(exp) }
      | APPROXIMATE ~> "(" ~> floatLit ~ ")" ~ COUNT ~ "(" ~ DISTINCT ~ expression <~ ")" ^^
      { case s ~ _ ~ _ ~ _ ~ _ ~ e => ApproxCountDistinct(e, s.toDouble) }
      | FIRST ~ "(" ~> expression <~ ")" ^^ { case exp => First(exp) }
      | LAST  ~ "(" ~> expression <~ ")" ^^ { case exp => Last(exp) }
      | AVG   ~ "(" ~> expression <~ ")" ^^ { case exp => Average(exp) }
      | MIN   ~ "(" ~> expression <~ ")" ^^ { case exp => Min(exp) }
      | MAX   ~ "(" ~> expression <~ ")" ^^ { case exp => Max(exp) }
      | UPPER ~ "(" ~> expression <~ ")" ^^ { case exp => Upper(exp) }
      | LOWER ~ "(" ~> expression <~ ")" ^^ { case exp => Lower(exp) }
      | IF ~ "(" ~> expression ~ ("," ~> expression) ~ ("," ~> expression) <~ ")" ^^
      { case c ~ t ~ f => If(c, t, f) }
      | CASE ~> expression.? ~ (WHEN ~> expression ~ (THEN ~> expression)).* ~
      (ELSE ~> expression).? <~ END ^^ {
      case casePart ~ altPart ~ elsePart =>
        val altExprs = altPart.flatMap { case whenExpr ~ thenExpr =>
          Seq(casePart.fold(whenExpr)(EqualTo(_, whenExpr)), thenExpr)
        }
        CaseWhen(altExprs ++ elsePart.toList)
    }
      | (SUBSTR | SUBSTRING) ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case s ~ p => Substring(s, p, Literal(Integer.MAX_VALUE)) }
      | (SUBSTR | SUBSTRING) ~ "(" ~> expression ~ ("," ~> expression) ~ ("," ~> expression) <~ ")" ^^
      { case s ~ p ~ l => Substring(s, p, l) }
      | COALESCE ~ "(" ~> repsep(expression, ",") <~ ")" ^^ { case exprs => Coalesce(exprs) }
      | SQRT  ~ "(" ~> expression <~ ")" ^^ { case exp => Sqrt(exp) }
      | ABS   ~ "(" ~> expression <~ ")" ^^ { case exp => Abs(exp) }
      | ident ~ ("(" ~> repsep(expression, ",")) <~ ")" ^^
      { case udfName ~ exprs => UnresolvedFunction(udfName, exprs) }
      )
  // scalastyle:on

  protected lazy val hierarchy: Parser[LogicalPlan] =
    HIERARCHY ~> "(" ~>
        (USING ~> relationFactor) ~
        (JOIN ~> PARENT ~> ident) ~ (ON ~> expression) ~
        (SEARCH ~> BY ~> ordering).? ~
        (START ~> WHERE ~> expression) ~
        (SET ~> ident <~ ")") ~
        (AS ~> ident) ^^ {
      case rel ~ ca ~ pexpr ~ sba ~ sw ~ nc ~ alias =>
       new Hierarchy(
         alias = alias,
         relation = rel,
         childAlias = ca,
         parenthoodExpression = pexpr,
         searchBy = sba.getOrElse(Seq()),
         startWhere = sw,
         nodeAttribute = UnresolvedAttribute(nc))
    }

  protected lazy val extract: Parser[Expression] =
    EXTRACT ~ "(" ~> dateIdLiteral ~ (FROM ~> expression) <~ ")" ^^ {
      case dFlag ~ d => Extract(dFlag, d)
    }

  protected lazy val dateIdLiteral: Parser[Literal] =
    (DAY ^^^ Literal(DAY.str, StringType)
      | MONTH ^^^ Literal(MONTH.str, StringType)
      | YEAR ^^^ Literal(YEAR.str, StringType)
      | HOUR ^^^ Literal(HOUR.str, StringType)
      | MINUTE ^^^ Literal(MINUTE.str, StringType)
      | SECOND ^^^ Literal(SECOND.str, StringType)
      )
}
