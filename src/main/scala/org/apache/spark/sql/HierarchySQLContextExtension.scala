package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{SimpleFunctionRegistry, ResolveHierarchy, Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object HierarchySQLContextExtension extends SQLContextExtension {

  override def registerFunctions(registry : FunctionRegistry) : Unit =
    RegisterHierarchyFunctions(registry)

  override def functionRegistry : Option[FunctionRegistry] =
    Some(new SimpleFunctionRegistry(caseSensitive = false))

  override def sqlParser : Option[SparkSQLParser] =
    Some({
      val fallback = new HierarchiesSQLParser()
      new SparkSQLParser(fallback(_))
    })

  override def resolutionRules(analyzer : Analyzer) : Seq[Rule[LogicalPlan]] =
    Seq(ResolveHierarchy(analyzer))


  override def strategies(planner : ExtendedPlanner) : Seq[Strategy] =
    Seq(HierarchyStrategy(planner))

}
