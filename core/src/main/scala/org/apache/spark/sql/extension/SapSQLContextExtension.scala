package org.apache.spark.sql.extension

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SapDDLStrategy, SystemTablesStrategy}
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

  protected def catalog: Catalog

  override protected def resolutionRules(analyzer: Analyzer): List[Rule[LogicalPlan]] =
    ResolveViews(analyzer) ::
    ResolveSystemTables(analyzer) ::
    ResolveReferencesWithHierarchies(analyzer) ::
    ResolveHierarchy(analyzer) ::
    ResolveStarAnnotations(analyzer) ::
    ResolveAnnotations(analyzer) ::
    ResolveTableFunctions(analyzer) ::
    ResolveCountDistinctStar(analyzer) ::
    ResolveDeepDescribe(analyzer) ::
    ResolveSelectWith(analyzer) ::
    ResolveDropCommand(analyzer, catalog) ::
    Nil

  override protected def optimizerEarlyBatches: List[ExtendableOptimizerBatch] =
    ExtendableOptimizerBatch(
      name = "Redundant pushable filters",
      iterations = 1,
      rules = BooleanSimplification :: RedundantDownPushableFilters :: Nil
    ) :: Nil

  override protected def optimizerMainBatchRules: List[Rule[LogicalPlan]] =
    FiltersReduction :: AssureRelationsColocality :: Nil

  override protected def strategies(planner: ExtendedPlanner): List[Strategy] =
    SapDDLStrategy(planner) ::
    CreateTableStrategy ::
    CatalystSourceStrategy ::
    HierarchyStrategy(planner) ::
    TableFunctionsStrategy(planner) ::
    RawSqlSourceStrategy ::
    SystemTablesStrategy(planner) :: Nil

  override protected def extendedParserDialect: ParserDialect = new SapParserDialect

  override protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser =
    new SapDDLParser(parser)

  override protected def registerFunctions(registry: FunctionRegistry): Unit = {
    RegisterHierarchyFunctions(registry)
    RegisterCustomFunctions(registry)
  }

}
