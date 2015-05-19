package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, ResolveHierarchy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

private[sql] trait SQLContextHierarchiesExtension
  extends SQLContextRegisterFunctions
  with SQLContextParserExtension
  with SQLContextAnalyzerExtension
  with SQLContextPlannerExtension {

  override def registerFunctions(registry : FunctionRegistry) : Unit = {
    super.registerFunctions(registry)
    RegisterHierarchyFunctions(registry)
  }

  override def extendedSqlParser : Option[SparkSQLParser] =
    Some({
      val fallback = new HierarchiesSQLParser()
      new SparkSQLParser(fallback(_))
    })

  override def resolutionRules(analyzer : Analyzer) : List[Rule[LogicalPlan]] =
    ResolveHierarchy(analyzer) :: super.resolutionRules(analyzer)


  override def strategies(planner : ExtendedPlanner) : List[Strategy] =
    HierarchyStrategy(planner) :: super.strategies(planner)

}
