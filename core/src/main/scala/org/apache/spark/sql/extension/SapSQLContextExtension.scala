package org.apache.spark.sql.extension

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SapDDLStrategy
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.tablefunctions.TableFunctionsStrategy
import org.apache.spark.sql.hierarchy.HierarchyStrategy

/**
  * Provides every SAP Spark extension ready to be mixed in with contexts.
  * This is used both with [[SapSQLContext]] and [[org.apache.spark.sql.hive.SapHiveContext]].
  *
  * @see [[SQLContextExtension]]
  */
private[sql] trait SapSQLContextExtension extends SQLContextExtension {

  override protected def resolutionRules(analyzer: Analyzer): List[Rule[LogicalPlan]] =
    ResolveReferencesWithHierarchies(analyzer) ::
    ResolveHierarchy(analyzer) ::
    ResolveStarAnnotations(analyzer) ::
    ResolveAnnotations ::
    ResolveTableFunctions(analyzer) ::
    ResolveDeepDescribe(analyzer) ::
    Nil

  override protected def optimizerEarlyBatches: List[ExtendableOptimizerBatch] =
    ExtendableOptimizerBatch(
      name = "Redundant pushable filters",
      iterations = 1,
      rules = BooleanSimplification :: RedundantDownPushableFilters :: Nil
    ) :: Nil

  override protected def optimizerMainBatchRules: List[Rule[LogicalPlan]] =
    FiltersReduction :: Nil

  override protected def strategies(planner: ExtendedPlanner): List[Strategy] =
    SapDDLStrategy(planner) ::
    CreatePersistentTableStrategy ::
    CatalystSourceStrategy ::
    HierarchyStrategy(planner) ::
    TableFunctionsStrategy(planner) :: Nil

  override protected def extendedParserDialect: ParserDialect = new SapParserDialect

  override protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser =
    new SapDDLParser(parser)

  override protected def registerFunctions(registry: FunctionRegistry): Unit = {
    RegisterHierarchyFunctions(registry)
    RegisterCustomFunctions(registry)
  }

}
