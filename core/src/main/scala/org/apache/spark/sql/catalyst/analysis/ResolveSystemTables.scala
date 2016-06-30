package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.systables.SystemTableRegistry
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedSystemTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
  * Resolves [[UnresolvedSystemTable]]s.
  *
  * @param analyzer The [[Analyzer]]
  * @param sqlContext The Spark [[SQLContext]]
  */
case class ResolveSystemTables(
    analyzer: Analyzer,
    sqlContext: SQLContext)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case table: UnresolvedSystemTable =>
      LogicalRelation(SystemTableRegistry.resolve(table, sqlContext))
  }
}
