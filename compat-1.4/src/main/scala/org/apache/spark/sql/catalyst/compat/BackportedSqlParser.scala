package org.apache.spark.sql.catalyst.compat

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._

import scala.language.implicitConversions

// scalastyle:off

class BackportedSqlParser extends SqlParser {
  protected lazy val originalFunction: Parser[Expression] =
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
      { case c ~ t ~ f => FixedIf(c, t, f) }
      | CASE ~> expression.? ~ rep1(WHEN ~> expression ~ (THEN ~> expression)) ~
      (ELSE ~> expression).? <~ END ^^ {
      case casePart ~ altPart ~ elsePart =>
        val branches = altPart.flatMap { case whenExpr ~ thenExpr =>
          Seq(whenExpr, thenExpr)
        } ++ elsePart
        casePart.map(CaseKeyWhen(_, branches)).getOrElse(CaseWhen(branches))
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

}
