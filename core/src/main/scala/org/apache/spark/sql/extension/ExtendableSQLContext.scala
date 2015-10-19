package org.apache.spark.sql.extension

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{ParserDialect, CatalystConf}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.execution.ExtractPythonUdfs
import org.apache.spark.sql.hive.{HiveContext, HiveFunctionRegistryProxy, HiveMetastoreCatalogProxy, HiveStrategiesProxy}
import org.apache.spark.sql.sources.{DDLParser, DataSourceStrategy}

/**
 * Extendable SQLContext. This SQLContext is composable with traits
 * that can provide extended parsers, resolution rules, function registries or
 * strategies.
 *
 * @param sparkContext The SparkContext.
 */
@DeveloperApi
private[sql] class ExtendableSQLContext(
    @transient override val sparkContext: SparkContext)
  extends HiveContext(sparkContext)
  with SQLContextExtensionBase {
  self =>

  override protected[sql] def getSQLDialect(): ParserDialect = extendedParserDialect

  @transient
  override protected[sql] val ddlParser: DDLParser = extendedDdlParser(sqlParser.parse)

  @transient
  override protected[sql] lazy val functionRegistry = {
    val registry = new HiveFunctionRegistryProxy with OverrideFunctionRegistry {

      override val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive = false)

      override def conf: CatalystConf = currentSession().conf
    }
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
  override protected[sql] lazy val catalog = new HiveMetastoreCatalogProxy(metadataHive, this)

  @transient
  override protected[sql] lazy val analyzer: Analyzer = {

    new Analyzer(catalog, functionRegistry, conf) with CheckAnalysis {
      val parentRules =
        catalog.ParquetConversions ::
        catalog.CreateTables ::
        ExtractPythonUdfs ::
        sources.PreInsertCastAndRename ::
        Nil
      override val extendedResolutionRules = resolutionRules(this) ++ parentRules

      /* XXX: Override this to replace ResolveReferences with ResolveReferencesWithHierarchies */
      override lazy val batches: Seq[Batch] = Seq(
        Batch("Substitution", fixedPoint,
          CTESubstitution ::
            WindowsSubstitution ::
            Nil: _*),
        Batch("Resolution", fixedPoint,
          ResolveRelations ::
            ResolveReferencesWithHierarchies(this) ::
            ResolveGroupingAnalytics ::
            ResolveSortReferences ::
            ResolveGenerate ::
            ResolveFunctions ::
            ExtractWindowExpressions ::
            GlobalAggregates ::
            UnresolvedHavingClauseAttributes ::
            TrimGroupingAliases ::
            typeCoercionRules ++
              extendedResolutionRules: _*)
      )

      override val extendedCheckRules = Seq(
        sources.PreWriteCheck(catalog)
      )
    }
  }

  @transient
  override protected[sql] lazy val optimizer: Optimizer = new Optimizer {

    private val extendedOptimizerRules = self.optimizerRules
    private val MAX_ITERATIONS = 100

    /* TODO: This should be gone in Spark 1.5+
     * See: https://issues.apache.org/jira/browse/SPARK-7727
     */
    // scalastyle:off structural.type
    private def transformBatchType(b: DefaultOptimizer.Batch): Batch = {
      val strategy = b.strategy.maxIterations match {
        case 1 => Once
        case n => FixedPoint(n)
      }
      Batch(b.name, strategy, b.rules: _*)
    }

    // scalastyle:on structural.type

    private val baseBatches = DefaultOptimizer.batches.map(transformBatchType)

    override protected val batches: Seq[Batch] = if (extendedOptimizerRules.isEmpty) {
      baseBatches
    } else {
      baseBatches :+
        Batch("Extended Optimization Rules", FixedPoint(MAX_ITERATIONS), extendedOptimizerRules: _*)
    }
  }

  @transient
  override protected[sql] val planner =
  // HiveStrategies defines its own strategies, we should be back to SparkPlanner strategies
    new SparkPlanner with HiveStrategiesProxy with ExtendedPlanner {
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
