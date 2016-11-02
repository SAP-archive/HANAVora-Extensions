package org.apache.spark.sql.extension

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{ParserDialect, SimpleCatalystConf}
import org.apache.spark.sql.execution.ExtractPythonUDFs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql._

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

  override protected def extendedCheckRules(analyzer: Analyzer): Seq[(LogicalPlan) => Unit] =
    RecursiveViewAnalysis.apply _ ::
      PreWriteCheck(catalog) ::
      HierarchyAnalysis(catalog) :: Nil

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

      override val extendedCheckRules = ExtendableSQLContext.this.extendedCheckRules(this)
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
    OptimizerFactory.produce(
      earlyBatches = optimizerEarlyBatches,
      mainBatchRules = optimizerMainBatchRules,
      postBatches = optimizerPostBatches
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
          Aggregation ::
          LeftSemiJoin ::
          EquiJoinSelection ::
          InMemoryScans ::
          BasicOperators ::
          BroadcastNestedLoop ::
          CartesianProduct ::
          DefaultJoin :: Nil

      override def strategies: Seq[Strategy] =
        self.strategies(this) ++
          experimental.extraStrategies ++
          baseStrategies
    }
}
