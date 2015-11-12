package org.apache.spark.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.compat._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, _}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.{CatalystConf, ParserDialect}
import org.apache.spark.sql.execution.ExtractPythonUdfs
import org.apache.spark.sql.extension._
import org.apache.spark.sql.sources.{DDLParser, DataSourceStrategy}

/**
 * Extendable [[HiveContext]]. This context is composable with traits
 * that can provide extended parsers, resolution rules, function registries or
 * strategies.
 *
 * @param sparkContext The SparkContext.
 */
@DeveloperApi
private[hive] class ExtendableHiveContext(@transient override val sparkContext: SparkContext)
  extends HiveContext(sparkContext) with SQLContextExtensionBase {
  self =>

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
  override protected[sql] lazy val functionRegistry = {
    val registry = new HiveFunctionRegistry with OverrideFunctionRegistry {

      override val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive = false)

      override def conf: CatalystConf = currentSession().conf
    }
    registry.registerBuiltins()
    registerFunctions(registry)
    registry
  }

  override protected[sql] def createSession(): SQLSession = {
    new this.SQLSession()
  }

  protected class SQLSession extends super.SQLSession {
    override protected[sql] lazy val conf: SQLConf = new SQLConf {
      override def caseSensitiveAnalysis: Boolean =
        getConf(SQLConf.CASE_SENSITIVE, "true").toBoolean
    }
  }

  @transient
  override protected[sql] lazy val catalog =
    new HiveMetastoreCatalog(metadataHive, this)
      with OverrideCatalog with TemporaryFlagProxyCatalog

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
          ExtractPythonUdfs ::
          ResolveHiveWindowFunction ::
          sources.PreInsertCastAndRename ::
          Nil)

      override val extendedCheckRules = Seq(
        sources.PreWriteCheck(catalog),
        // TODO: Move this once bug #95571 is fixed.
        HierarchyUDFAnalysis(catalog)
      )
    }

  @transient
  override protected[sql] lazy val optimizer: Optimizer =
    new ExtendableOptimizer(
      earlyBatches = optimizerEarlyBatches,
      mainBatchRules = optimizerMainBatchRules
    )

  @transient
  override protected[sql] val planner: SparkPlanner with HiveStrategies =
  // HiveStrategies defines its own strategies, we should be back to SparkPlanner strategies
    new SparkPlanner with HiveStrategies with ExtendedPlanner {
      override def strategies: Seq[Strategy] = self.strategies(this) ++
        experimental.extraStrategies ++ (
        DataSourceStrategy ::
          DDLStrategy ::
          TakeOrdered ::
          HashAggregation ::
          LeftSemiJoin ::
          HashJoin ::
          InMemoryScans ::
          ParquetOperations ::
          BasicOperators ::
          CartesianProduct ::
          BroadcastNestedLoopJoin ::
          HiveTableScans :: Nil)

      override val hiveContext = self
    }
}
