package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SelectUsing, UnresolvedSelectUsing}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.RawSqlSourceProvider
import org.apache.spark.sql.{DatasourceResolver, SQLContext}

/**
  * Resolves [[org.apache.spark.sql.catalyst.plans.logical.UnresolvedSelectUsing]] to
  * [[org.apache.spark.sql.catalyst.plans.logical.SelectUsing]]
  */
private[sql] case class ResolveSelectUsing(sqlContext: SQLContext) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UnresolvedSelectUsing(sqlCommand, provider, expectedSchema, options) => {
      val resolver = DatasourceResolver.resolverFor(sqlContext)
      val rawSqlProvider = resolver.newInstanceOfTyped[RawSqlSourceProvider](provider)
      val execution = rawSqlProvider.executionOf(sqlContext, options, sqlCommand, expectedSchema)
      SelectUsing(execution)
    }
  }

}
