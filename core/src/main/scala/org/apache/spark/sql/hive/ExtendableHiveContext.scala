package org.apache.spark.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, _}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{CacheManager, ExtractPythonUDFs}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.extension._
import org.apache.spark.sql.hive.client.{ClientInterface, ClientWrapper}

/**
 * Extendable [[HiveContext]]. This context is composable with traits
 * that can provide extended parsers, resolution rules, function registries or
 * strategies.
 *
 * @param sparkContext The SparkContext.
 */
@DeveloperApi
private[hive] class ExtendableHiveContext(
    @transient override val sparkContext: SparkContext,
    cacheManager: CacheManager,
    listener: SQLListener,
    @transient protected val execHive: ClientWrapper,
    @transient protected val metaHive: ClientInterface,
    isRootContext: Boolean)
  extends HiveContext(
    sparkContext,
    cacheManager,
    listener,
    execHive,
    metaHive,
    isRootContext)
  with SQLContextExtensionBase {

  self =>

  def this(sc: SparkContext) =
    this(sc, new CacheManager, SQLContext.createListenerAndUI(sc), null, null, true)

  // register the functions on instantiation
  registerBuiltins(functionRegistry)
  registerFunctions(functionRegistry)

  override protected[sql] def getSQLDialect(): ParserDialect = extendedParserDialect

  @transient
  override protected[sql] val ddlParser: DDLParser = extendedDdlParser(sqlParser.parse)

  override def sql(sqlText: String): DataFrame = {
    /* TODO: Switch between SQL dialects (Spark SQL's, HiveQL or our extended parser. */
    DataFrame(this,
      ddlParser.parse(sqlText, exceptionOnError = false)
    )
  }

  @transient
  override protected[sql] lazy val catalog =
    new HiveMetastoreCatalog(metadataHive, this)
      with OverrideCatalog with TemporaryFlagProxyCatalog


  override protected def extendedCheckRules(analyzer: Analyzer): Seq[(LogicalPlan) => Unit] =
    RecursiveViewAnalysis.apply _ ::
      PreWriteCheck(catalog) ::
      HierarchyAnalysis(catalog) ::
      Nil

  /**
   * Copy of [[HiveContext]]'s [[Analyzer]] adding rules from our extensions.
   */
  @transient
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules = resolutionRules(this) ++
        (catalog.ParquetConversions ::
          catalog.CreateTables ::
          catalog.PreInsertionCasts ::
          ExtractPythonUDFs ::
          ResolveHiveWindowFunction ::
          PreInsertCastAndRename ::
          Nil)

      override val extendedCheckRules = ExtendableHiveContext.this.extendedCheckRules(this)
    }

  @transient
  override protected[sql] lazy val optimizer: Optimizer =
    new ExtendableOptimizer(
      earlyBatches = optimizerEarlyBatches,
      mainBatchRules = optimizerMainBatchRules,
      postBatches = optimizerPostBatches
    )

  @transient
  override protected[sql] val planner: SparkPlanner with HiveStrategies =
    new SparkPlanner with HiveStrategies with ExtendedPlanner {
      def baseStrategies(hiveContext: HiveContext): Seq[Strategy] =
        Seq(
          DataSourceStrategy,
          HiveCommandStrategy(self),
          HiveDDLStrategy,
          DDLStrategy,
          TakeOrderedAndProject,
          InMemoryScans,
          HiveTableScans,
          DataSinks,
          Scripts,
          Aggregation,
          LeftSemiJoin,
          EquiJoinSelection,
          BasicOperators,
          BroadcastNestedLoop,
          CartesianProduct,
          DefaultJoin
        )

      override def strategies: Seq[Strategy] =
        self.strategies(this) ++
          experimental.extraStrategies ++
          baseStrategies(self)

      override val hiveContext = self
    }
}
