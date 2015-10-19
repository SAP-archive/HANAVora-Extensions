package org.apache.spark.sql.extension

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, Analyzer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.DDLParser

/**
 * An extension for a [[SQLContext]]. This is to be used in combination
 * with [[ExtendableSQLContext]].
 */
@DeveloperApi
private[sql] trait SQLContextExtension {

  /**
   * Additional resolution rules for the [[Analyzer]].
   *
   * @param analyzer The analyzer.
   * @return A list with additional resolution rules.
   */
  protected def resolutionRules(analyzer: Analyzer): List[Rule[LogicalPlan]]

  /**
   * Additional resolution rules for the [[org.apache.spark.sql.catalyst.optimizer.Optimizer]].
   *
   * @return A list of optimization rules.
   */
  protected def optimizerRules: List[Rule[LogicalPlan]]

  /**
   * Additional planning strategies.
   *
   * @param planner An [[ExtendedPlanner]] that a strategy might use to delegate a subtree.
   * @return A list of planning strategies.
   */
  protected def strategies(planner: ExtendedPlanner): List[Strategy]

  /**
   * A [[ParserDialect]] providing support for a SQL dialect.
   */
  protected def extendedParserDialect: ParserDialect

  /**
   * Provides a [[DDLParser]].
   *
   * @param parser Fallback parser.
   * @return
   */
  protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser

  /**
   * Runs anyh code on the [[FunctionRegistry]]. This is useful
   * to register custom functions.
   *
   * @param registry The [[FunctionRegistry]]
   */
  protected def registerFunctions(registry: FunctionRegistry): Unit

}
