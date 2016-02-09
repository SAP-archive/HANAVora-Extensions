package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.tablefunctions.UnresolvedTableFunction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class ResolveTableFunctions(
    analyzer: Analyzer, registry: TableFunctionRegistry = TableFunctionRegistry)
  extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case UnresolvedTableFunction(name, arguments) =>
      val lookup = registry.lookupFunction(name)
      lookup match {
        case Some(f) =>
          val analyzed = f.analyze(analyzer, arguments)
          ResolvedTableFunction(f, analyzed)
        case None =>
          throw new AnalysisException(s"Undefined function $name")
      }
  }
}
