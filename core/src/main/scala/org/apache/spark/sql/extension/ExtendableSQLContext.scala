package org.apache.spark.sql.extension

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.{ParserDialect, SimpleCatalystConf}
import org.apache.spark.sql.execution.ExtractPythonUDFs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.catalyst.analysis.SimpleFunctionRegistry

import scala.util.Try

/**
  * An [[SQLContext]] that eases extensions by mixin [[SQLContextExtension]].
  *
  * @see [[SapSQLContext]]
  *
  * @param sparkContext The SparkContext.
  */
private[sql] class ExtendableSQLContext(@transient override val sparkContext: SparkContext)
  extends SQLContext(sparkContext) with SQLContextExtensionBase {
  self =>

  /** Override SQL parser using [[SQLContextExtension.extendedParserDialect]]. */
  override protected[sql] def getSQLDialect(): ParserDialect = extendedParserDialect

  /** Override SQL DDL parser using [[SQLContextExtension.extendedDdlParser()]]. */
  @transient
  override protected[sql] val ddlParser: DDLParser = extendedDdlParser(sqlParser.parse)

  private def catalystConf = new SimpleCatalystConf(caseSensitiveAnalysis = false)

  /**
    * Use a [[org.apache.spark.sql.catalyst.analysis.SimpleFunctionRegistry]]
    * (the default one) with any extra functions already registered by using
    * [[SQLContextExtension.registerFunctions]].
    */
  @transient
  override protected[sql] lazy val functionRegistry = {
    val registry = new SimpleFunctionRegistry()
    registerBuiltins(registry)
    registerFunctions(registry)
    registry
  }

  /**
   * Drops the temporary table with the given table name in the catalog. If the table has been
   * cached/persisted before, it's also unpersisted. If there is an error in the cache
   * resolution, still the table is dropped from the catalog.
   *
   * @param tableName the name of the table to be unregistered.
   */
  override def dropTempTable(tableName: String): Unit = {
    Try(cacheManager.tryUncacheQuery(table(tableName)))
    catalog.unregisterTable(Seq(tableName))
  }

  /**
    * Use a [[SimpleCatalog]] (Spark default) mixed in with our [[TemporaryFlagProxyCatalog]].
    *
    * NOTE: This could be moved to [[SQLContextExtension]].
    */
  @transient
  override protected[sql] lazy val catalog = new SimpleCatalog(conf) with TemporaryFlagProxyCatalog

  /**
    * We provide an [[Analyzer]] that mimicks [[SQLContext]]'s, but prepending other
    * resolution rules as defined by [[SQLContextExtension.resolutionRules()]].
    */
  @transient
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        resolutionRules(this) ++
          (ExtractPythonUDFs ::
          PreInsertCastAndRename ::
          Nil)

      override val extendedCheckRules = Seq(
        PreWriteCheck(catalog),
        // TODO: Move this once bug #95571 is fixed.
        HierarchyUDFAnalysis(catalog)
      )
    }

  /**
    * This [[Optimizer]] mimicks [[SQLContext]]'s, but adding
    * [[SQLContextExtension.optimizerEarlyBatches]] and
    * [[SQLContextExtension.optimizerMainBatchRules]].
    *
    * @see [[ExtendableOptimizer]]
    */
  @transient
  override protected[sql] lazy val optimizer: Optimizer =
    new ExtendableOptimizer(
      earlyBatches = optimizerEarlyBatches,
      mainBatchRules = optimizerMainBatchRules
    )

  /**
    * This [[SparkPlanner]] mimicks [[SQLContext]]'s, prepending
    * [[SQLContextExtension.strategies()]].
    */
  @transient
  override protected[sql] val planner =
  // HiveStrategies defines its own strategies, we should be back to SparkPlanner strategies
    new SparkPlanner with ExtendedPlanner {

      def baseStrategies: Seq[Strategy] =
        DataSourceStrategy ::
          DDLStrategy ::
          TakeOrderedAndProject ::
          HashAggregation ::
          Aggregation ::
          LeftSemiJoin ::
          EquiJoinSelection ::
          InMemoryScans ::
          BasicOperators ::
          CartesianProduct ::
          BroadcastNestedLoopJoin :: Nil

      override def strategies: Seq[Strategy] =
        self.strategies(this) ++
          experimental.extraStrategies ++
          baseStrategies
    }
}
