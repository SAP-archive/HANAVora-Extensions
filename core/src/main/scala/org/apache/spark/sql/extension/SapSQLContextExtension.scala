package org.apache.spark.sql.extension

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SapDDLStrategy
import org.apache.spark.sql.hierarchy.HierarchyStrategy
import org.apache.spark.sql.sources._

@DeveloperApi
private[sql] trait SapSQLContextExtension extends SQLContextExtension {

  override protected def resolutionRules(analyzer: Analyzer): List[Rule[LogicalPlan]] = List(
    ResolveReferencesWithHierarchies(analyzer),
    ResolveHierarchy(analyzer)
  )

  override protected def optimizerRules: List[Rule[LogicalPlan]] = Nil

  override protected def strategies(planner: ExtendedPlanner): List[Strategy] = List(
    SapDDLStrategy(planner),
    CreatePersistentTableStrategy,
    CatalystSourceStrategy,
    HierarchyStrategy(planner)
  )

  override protected def extendedParserDialect: ParserDialect = new SapParserDialect

  override protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser =
    new SapDDLParser(parser)

  override protected def registerFunctions(registry: FunctionRegistry): Unit = {
    RegisterHierarchyFunctions(registry)
  }

}
