package org.apache.spark.sql

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}
import org.apache.spark.sql.types.StringType
import java.util.Calendar
import org.apache.spark.sql.types.DoubleType

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

  protected val DAYOFMONTH = Keyword("DAYOFMONTH")
  protected val WEEKDAY = Keyword("WEEKDAY")
  protected val ADD_DAYS = Keyword("ADD_DAYS")
  protected val ADD_MONTHS = Keyword("ADD_MONTHS")
  protected val ADD_YEARS = Keyword("ADD_YEARS")
  protected val DAYS_BETWEEN = Keyword("DAYS_BETWEEN")
  protected val CURRENT_DATE = Keyword("CURRENT_DATE")
  protected val CURDATE = Keyword("CURDATE")
  protected val TRIM = Keyword("TRIM")
  protected val LTRIM = Keyword("LTRIM")
  protected val RTRIM = Keyword("RTRIM")
  protected val LPAD = Keyword("LPAD")
  protected val RPAD = Keyword("RPAD")
  protected val LENGTH = Keyword("LENGTH")
  protected val CONCAT = Keyword("CONCAT")
  protected val LOCATE = Keyword("LOCATE")
  protected val REPLACE = Keyword("REPLACE")
  protected val REVERSE = Keyword("REVERSE")
  
  protected val LN = Keyword("LN")
  protected val LOG = Keyword("LOG")
  protected val COS = Keyword("COS")
  protected val SIN = Keyword("SIN")
  protected val TAN = Keyword("TAN")
  protected val ACOS = Keyword("ACOS")
  protected val ASIN = Keyword("ASIN")
  protected val ATAN = Keyword("ATAN")
  protected val CEIL = Keyword("CEIL")
  protected val FLOOR = Keyword("FLOOR")
  protected val POWER = Keyword("POWER")
  protected val ROUND = Keyword("ROUND")
  protected val SIGN = Keyword("SIGN")
  protected val MOD = Keyword("MOD")
  protected val TO_DOUBLE = Keyword("TO_DOUBLE")
  protected val TO_INTEGER = Keyword("TO_INTEGER")
  protected val TO_VARCHAR = Keyword("TO_VARCHAR")

  lexical.delimiters +=(
    "$","@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
    )
  
  override protected lazy val relation: Parser[LogicalPlan] =
    hierarchy | joinedRelation | relationFactor

  override protected lazy val function: Parser[Expression] =
    extract | sparkFunctions | velocityFunctions | dataSourceFunctions

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

  protected lazy val dataSourceFunctions: Parser[Expression] =
     (
      "$" ~> ident ~ ("(" ~> repsep(expression,",") <~ ")") ^^
        { case udf~expr => DataSourceExpression(udf,expr) }
      )
  // scalastyle:off
  protected lazy val velocityFunctions: Parser[Expression] =
      (LENGTH ~ "(" ~> expression <~ ")" ^^ { case exp => Length(exp) }
      | TRIM  ~ "(" ~> expression <~ ")" ^^ { case exp => Trim(exp) }
      | LTRIM ~ "(" ~> expression <~ ")" ^^ { case exp => LTrim(exp) }
      | RTRIM ~ "(" ~> expression <~ ")" ^^ { case exp => RTrim(exp) }
      | LPAD ~ "(" ~> expression ~ ("," ~> expression) ~ ("," ~> expression) <~ ")" ^^
      { case s ~ l ~ p => LPad(s,l,p) }
      | LPAD ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case s ~ l => LPad(s,l,null) }
      | RPAD ~ "(" ~> expression ~ ("," ~> expression) ~ ("," ~> expression) <~ ")" ^^ 
      { case s ~ l ~ p => RPad(s,l,p) }
      | RPAD ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^ 
      { case s ~ l => RPad(s,l,null) }
      | TO_DOUBLE ~ "(" ~> expression <~ ")" ^^ { case exp => Cast(exp, DoubleType) }
      | TO_INTEGER ~ "(" ~> expression <~ ")" ^^ { case exp => ToInteger(exp) }
      | CONCAT ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case e1 ~ e2 => Concat(e1,e2) }
      | LOCATE ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case s ~ p => Locate(s,p) }
      | REPLACE ~ "(" ~> expression ~ ("," ~> expression) ~ ("," ~> expression) <~ ")" ^^
      { case s ~ f ~ p => Replace(s,f,p) }
      | REVERSE ~ "(" ~> expression <~ ")" ^^ { case s => Reverse(s) }

      | TO_VARCHAR ~ "(" ~> expression <~ ")" ^^ { case exp => ToVarChar(exp) }
      | LN   ~ "(" ~> expression <~ ")" ^^ { case exp => Ln(exp) }
      | LOG   ~ "(" ~> expression <~ ")" ^^ { case exp => Log(exp) }
      | COS   ~ "(" ~> expression <~ ")" ^^ { case exp => Cos(exp) }
      | SIN   ~ "(" ~> expression <~ ")" ^^ { case exp => Sin(exp) }
      | TAN   ~ "(" ~> expression <~ ")" ^^ { case exp => Tan(exp) }
      | ACOS   ~ "(" ~> expression <~ ")" ^^ { case exp => Acos(exp) }
      | ASIN   ~ "(" ~> expression <~ ")" ^^ { case exp => Asin(exp) }
      | ATAN   ~ "(" ~> expression <~ ")" ^^ { case exp => Atan(exp) }
      | CEIL   ~ "(" ~> expression <~ ")" ^^ { case exp => Ceil(exp) }
      | ROUND  ~ "(" ~> expression ~ ("," ~> expression)  <~ ")" ^^
      { case e ~ d => Round(e,d) }
      | POWER  ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case e ~ p => Power(e,p) }
      | MOD    ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case e ~ m => Remainder(e,m) }
      | SIGN   ~ "(" ~> expression <~ ")" ^^ { case exp => Sign(exp) }
      | FLOOR   ~ "(" ~> expression <~ ")" ^^ { case exp => Floor(exp) }

      | (CURDATE | CURRENT_DATE) ~ "(" ~ ")" ^^ { case exp => CurDate() }
      | DAYOFMONTH ~ "(" ~> expression <~ ")" ^^
      { case exp => DatePart(exp, Calendar.DAY_OF_MONTH) }
      | MONTH  ~ "(" ~> expression <~ ")" ^^
      { case exp => DatePart(exp, Calendar.MONTH) }
      | YEAR   ~ "(" ~> expression <~ ")" ^^
      { case exp => DatePart(exp, Calendar.YEAR) }
      | HOUR   ~ "(" ~> expression <~ ")" ^^
      { case exp => DatePart(exp, Calendar.HOUR_OF_DAY) }
      | MINUTE ~ "(" ~> expression <~ ")" ^^
      { case exp => DatePart(exp, Calendar.MINUTE) }
      | SECOND ~ "(" ~> expression <~ ")" ^^
      { case exp => DatePart(exp, Calendar.SECOND) }
      | ADD_DAYS ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case e ~ d => AddDays(e,d) }
      | ADD_MONTHS ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case e ~ m => AddMonths(e,m) }
      | ADD_YEARS ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case e ~ y => AddYears(e,y) }
      | DAYS_BETWEEN ~ "(" ~> expression ~ ("," ~> expression) <~ ")" ^^
      { case d1 ~ d2 => DaysBetween(d1,d2) }
      )
  // scalastyle:on
}
