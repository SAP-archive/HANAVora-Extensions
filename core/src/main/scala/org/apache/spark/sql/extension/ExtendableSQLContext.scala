package org.apache.spark.sql.extension

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, ParserDialect}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, SimpleCatalog, SimpleFunctionRegistry}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.execution.ExtractPythonUdfs
import org.apache.spark.sql.sources.{DDLParser, PreInsertCastAndRename}

/**
 * Extendable [[SQLContext]]. This context is composable with traits
 * that can provide extended parsers, resolution rules, function registries or
 * strategies.
 *
 * @param sparkContext The SparkContext.
 */
private[sql] class ExtendableSQLContext(@transient override val sparkContext: SparkContext)
  extends SQLContext(sparkContext) with SQLContextExtensionBase {
  self =>

  override protected[sql] def getSQLDialect(): ParserDialect = extendedParserDialect

  @transient
  override protected[sql] val ddlParser: DDLParser = extendedDdlParser(sqlParser.parse)

  private def catalystConf = new SimpleCatalystConf(caseSensitiveAnalysis = false)

  @transient
  override protected[sql] lazy val functionRegistry = {
    val registry = new SimpleFunctionRegistry(catalystConf)
    registerFunctions(registry)
    registry
  }

  protected class SQLSession extends super.SQLSession {
    override protected[sql] lazy val conf: SQLConf = new SQLConf {
      override def caseSensitiveAnalysis: Boolean =
        getConf(SQLConf.CASE_SENSITIVE, "true").toBoolean
    }
  }

  @transient
  override protected[sql] lazy val catalog = new SimpleCatalog(conf) with TemporaryFlagProxyCatalog

  /**
   * Copy of [[SQLContext]]'s [[Analyzer]] adding rules from our extensions.
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
        // TODO: Move this once bug #95571 is fixed.
        HierarchyUDFAnalysis(catalog)
      )
    }

  @transient
  override protected[sql] lazy val optimizer: Optimizer =
    new ExtendableOptimizer(optimizerRules)

  @transient
  override protected[sql] val planner =
  // HiveStrategies defines its own strategies, we should be back to SparkPlanner strategies
    new SparkPlanner with ExtendedPlanner {
      override def strategies: Seq[Strategy] =
        self.strategies(this) ++ super.strategies
    }
}
