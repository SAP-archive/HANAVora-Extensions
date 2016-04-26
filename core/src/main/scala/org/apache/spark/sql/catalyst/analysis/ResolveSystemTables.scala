package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.analysis.systables.SystemTableRegistry
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedSystemTable}
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Resolves [[UnresolvedSystemTable]]s.
  *
  * @param analyzer The [[Analyzer]]
  */
case class ResolveSystemTables(analyzer: Analyzer) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case table: UnresolvedSystemTable =>
      SystemTableRegistry.resolve(table)
  }
}
