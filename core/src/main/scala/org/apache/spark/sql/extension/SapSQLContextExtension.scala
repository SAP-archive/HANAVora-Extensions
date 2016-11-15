package org.apache.spark.sql.extension

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SelfJoinStrategy
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.tablefunctions.TableFunctionsStrategy
import org.apache.spark.sql.extension.OptimizerFactory.ExtendableOptimizerBatch
import org.apache.spark.sql.hierarchy.HierarchyStrategy
import org.apache.spark.sql.parser.{SapDDLParser, SapParserDialect}

/**
  * Provides every SAP Spark extension ready to be mixed in with contexts.
  * This is used both with [[SapSQLContext]] and [[org.apache.spark.sql.hive.SapHiveContext]].
  *
  * @see [[SQLContextExtension]]
  */
private[sql] trait SapSQLContextExtension extends SQLContextExtension {
  this: SQLContext =>

  protected def catalog: Catalog

  override protected def resolutionRules(analyzer: Analyzer): List[Rule[LogicalPlan]] =
    FixCaseSensitivity(analyzer) ::
    ResolveViews(analyzer) ::
    ResolveSystemTables(analyzer, this) ::
    ResolveReferencesWithHierarchies(analyzer) ::
    ResolveHierarchy(analyzer) ::
    ExcludeHierarchyNodeFromSelectStar(analyzer) ::
    ResolveStarAnnotations(analyzer) ::
    ResolveAnnotations(analyzer) ::
    ResolveTableFunctions(analyzer) ::
    ResolveCountDistinctStar(analyzer) ::
    ResolveDeepDescribe(analyzer) ::
    ResolveSelectUsing(this) ::
    ResolveSparkLocalDropCommand(analyzer, catalog) ::
    ResolveDropUsing(this) ::
    ResolveInferSchemaCommand(this) ::
    ResolveAppendCommand(analyzer) ::
    Nil

  override protected def optimizerEarlyBatches: List[ExtendableOptimizerBatch] =
    ExtendableOptimizerBatch(
      name = "Redundant pushable filters",
      iterations = 1,
      rules = BooleanSimplification :: RedundantDownPushableFilters :: Nil
    ) :: Nil

  override protected def optimizerMainBatchRules: List[Rule[LogicalPlan]] =
    FiltersReduction :: AssureRelationsColocality :: Nil

  override protected def optimizerPostBatches: List[ExtendableOptimizerBatch] = Nil

  override protected def strategies(planner: ExtendedPlanner): List[Strategy] =
    CreateTableStrategy(this) ::
    CatalystSourceStrategy ::
    HierarchyStrategy(planner) ::
    TableFunctionsStrategy(planner) ::
    RawSqlSourceStrategy ::
    SelfJoinStrategy(planner) :: Nil

  override protected def extendedParserDialect: ParserDialect = new SapParserDialect

  override protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser =
    new SapDDLParser(parser)

  override protected def registerFunctions(registry: FunctionRegistry): Unit = {
    RegisterHierarchyFunctions(registry)
    RegisterCustomFunctions(registry)
  }

}
