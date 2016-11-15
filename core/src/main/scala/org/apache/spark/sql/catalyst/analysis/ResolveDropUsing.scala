package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DropUsingRunnableCommand
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.commands.UnresolvedProviderBoundDropCommand
import org.apache.spark.sql.{DatasourceResolver, SQLContext}

/**
  * Resolves [[UnresolvedProviderBoundDropCommand]] to [[DropUsingRunnableCommand]]
  * @param sqlContext The Spark [[SQLContext]]
  */
private[sql]
case class ResolveDropUsing(sqlContext: SQLContext)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case UnresolvedProviderBoundDropCommand(
        target, ifExists, tableIdentifier, cascade, options, provider) =>
      val dropProvider =
        DatasourceResolver.resolverFor(sqlContext).newInstanceOfTyped[DropProvider](provider)

      DropUsingRunnableCommand(dropProvider, target, ifExists, tableIdentifier, cascade, options)
  }
}
