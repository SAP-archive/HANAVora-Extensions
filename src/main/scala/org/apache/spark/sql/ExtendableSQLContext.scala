package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ExtractPythonUdfs, SparkPlan}
import org.apache.spark.sql.sources.DDLParser

/**
 * Extendable SQLContext. This SQLContext is composable with traits
 * that can provide extended parsers, resolution rules, function registries or
 * strategies.
 *
 * @param sparkContext The SparkContext.
 */
@DeveloperApi
class ExtendableSQLContext(@transient override val sparkContext: SparkContext)
  extends SQLContext(sparkContext)
  with SQLParserSQLContextExtension
  with RegisterFunctionsSQLContextExtension
  with AnalyzerSQLContextExtension
  with OptimizerSQLContextExtension
  with PlannerSQLContextExtension
  with DDLParserSQLContextExtension {
  self =>

  @transient
  override protected[sql] val sqlParser : SparkSQLParser = extendedSqlParser

  @transient
  override protected[sql] val ddlParser : DDLParser = extendedDdlParser(sqlParser.apply)

  @transient
  override protected[sql] lazy val functionRegistry: FunctionRegistry = {
    val registry = new SimpleFunctionRegistry(caseSensitive = false)
    registerFunctions(registry)
    registry
  }

  @transient
  override protected[sql] lazy val analyzer: Analyzer = {
    val parentRules =
      ExtractPythonUdfs ::
        sources.PreInsertCastAndRename ::
        Nil
    new Analyzer(catalog, functionRegistry, caseSensitive = true) {
      override val extendedResolutionRules = resolutionRules(this) ++ parentRules

      /* XXX: Override this to replace EliminateSubQueries with RewriteWithoutSubQueries */
      override lazy val batches: Seq[Batch] = Seq(
        Batch("Resolution", fixedPoint,
          ResolveRelations ::
            ResolveReferences ::
            ResolveGroupingAnalytics ::
            ResolveSortReferences ::
            ImplicitGenerate ::
            ResolveFunctions ::
            GlobalAggregates ::
            UnresolvedHavingClauseAttributes ::
            TrimGroupingAliases ::
            typeCoercionRules ++
              extendedResolutionRules : _*),
        Batch("Remove SubQueries", fixedPoint,
          RewriteWithoutSubQueries)
      )
    }
  }

  @transient
  override protected[sql] lazy val optimizer: Optimizer = new Optimizer {

    private val extendedOptimizerRules = self.optimizerRules
    private val MAX_ITERATIONS = 100

    /* TODO: This should be gone in Spark 1.4+
     * See: https://issues.apache.org/jira/browse/SPARK-7727
     */
    // scalastyle:off structural.type
    private def transformBatchType(b: DefaultOptimizer.Batch): Batch = {
      val strategy = b.strategy.maxIterations match {
        case 1 => Once
        case n => FixedPoint(n)
      }
      Batch(b.name, strategy, b.rules : _*)
    }
    // scalastyle:on structural.type

    private val baseBatches = DefaultOptimizer.batches.map(transformBatchType)

    override protected val batches: Seq[Batch] = if (extendedOptimizerRules.isEmpty) {
      baseBatches
    } else {
      baseBatches :+
        Batch("Extended Optimization Rules", FixedPoint(MAX_ITERATIONS), extendedOptimizerRules :_*)
    }
  }

  @transient
  override protected[sql] val planner: SparkPlanner = new SparkPlanner with ExtendedPlanner {
    override def strategies: Seq[Strategy] = self.strategies(this) ++ super.strategies
  }

}

@DeveloperApi
trait ExtendedPlanner {
  self: SQLContext#SparkPlanner =>
  def planLaterExt(p : LogicalPlan) : SparkPlan = self.planLater(p)

  def optimizedPlan(p: LogicalPlan): LogicalPlan = self.sqlContext.executePlan(p).optimizedPlan
}

@DeveloperApi
trait SQLParserSQLContextExtension {
  protected def extendedSqlParser: SparkSQLParser = {
    val fallback = new catalyst.SqlParser
    new SparkSQLParser(fallback(_))
  }
}

@DeveloperApi
trait DDLParserSQLContextExtension {
  protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser =
    new DDLParser(parser)
}

@DeveloperApi
trait RegisterFunctionsSQLContextExtension {
  protected def registerFunctions(registry: FunctionRegistry): Unit = { }
}

@DeveloperApi
trait AnalyzerSQLContextExtension {
  protected def resolutionRules(analyzer : Analyzer) : List[Rule[LogicalPlan]] = Nil
}

@DeveloperApi
trait OptimizerSQLContextExtension {
  protected def optimizerRules: List[Rule[LogicalPlan]] = Nil
}

@DeveloperApi
trait PlannerSQLContextExtension {
  protected def strategies(planner : ExtendedPlanner) : List[Strategy] = Nil
}
