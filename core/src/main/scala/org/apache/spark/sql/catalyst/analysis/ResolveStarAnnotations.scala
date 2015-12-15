package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Resolves a query that have a [[Star]] with [[AnnotatedAttribute]] by expanding
  * the asterisk and handing the resulting query to other resolvers.
  *
  * this makes it posssible to handle annotations correctly per columns.
  *
  * @param analyzer the analyzer.
 */
private[sql]
case class ResolveStarAnnotations(analyzer: Analyzer) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // If the projection list contains Stars, expand it.
      case p@Project((a@AnnotatedAttribute(s: Star)) :: Nil, child) =>
        Project(
          s.expand(child.output, analyzer.resolver).map(e => AnnotatedAttribute(e)(a.annotations)),
          child)
      case p@Project((a@AnnotationFilter(s: Star)) :: Nil, child) =>
        Project(
          s.expand(child.output, analyzer.resolver).map(e => AnnotationFilter(e)(a.filters)),
          child)
    }
  }
}
