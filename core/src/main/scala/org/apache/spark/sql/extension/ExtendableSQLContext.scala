package org.apache.spark.sql.extension

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, ParserDialect}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, SimpleCatalog, SimpleFunctionRegistry}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.execution.ExtractPythonUdfs
import org.apache.spark.sql.sources.{DDLParser, PreInsertCastAndRename}

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
    val registry = new SimpleFunctionRegistry(catalystConf)
    registerFunctions(registry)
    registry
  }

  /**
    * Override default SQL session to enforce different behavior
    * on some settings.
    *
    * This is needed in the case of Hive, but it could be
    * removed for this case in the future.
    */
  protected class SQLSession extends super.SQLSession {
    override protected[sql] lazy val conf: SQLConf = new SQLConf {
      override def caseSensitiveAnalysis: Boolean =
        getConf(SQLConf.CASE_SENSITIVE, "true").toBoolean
    }
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
          (ExtractPythonUdfs ::
          PreInsertCastAndRename ::
          Nil)

      override val extendedCheckRules = Seq(
        sources.PreWriteCheck(catalog),
        /* TODO: This belongs to SQLContextExtension. See bug #95571. */
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
      override def strategies: Seq[Strategy] =
        self.strategies(this) ++ super.strategies
    }
}
