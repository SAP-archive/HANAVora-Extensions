package org.apache.spark.sql

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}

class HierarchiesSQLParser extends SqlParser {

  protected val HIERARCHY       = Keyword("HIERARCHY")
  protected val USING           = Keyword("USING")
  protected val PARENT          = Keyword("PARENT")
  protected val SEARCH          = Keyword("SEARCH")
  protected val START           = Keyword("START")
  protected val SET             = Keyword("SET")

  override protected lazy val relation: Parser[LogicalPlan] =
    hierarchy | joinedRelation | relationFactor

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

}
