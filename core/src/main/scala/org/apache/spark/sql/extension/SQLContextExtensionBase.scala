package org.apache.spark.sql.extension

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{TableIdentifier, ParserDialect}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.errors.DialectException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DDLParser
import org.apache.spark.util.Utils

import scala.util.Try
import scala.util.control.NonFatal

/**
  * A default implementation for [[SQLContextExtension]]. This is always used by
  * [[ExtendableSQLContext]] to ease the use of stackable traits.
  */
private[sql] trait SQLContextExtensionBase extends SQLContextExtension {
  self: SQLContext =>

  /**
   * Drops the temporary table with the given table name in the catalog. If the table has been
   * cached/persisted before, it's also unpersisted. If there is an error in the cache
   * resolution, still the table is dropped from the catalog.
   *
   * @param tableName the name of the table to be unregistered.
   */
  override def dropTempTable(tableName: String): Unit = {
    Try(cacheManager.tryUncacheQuery(table(tableName)))
    catalog.unregisterTable(TableIdentifier(tableName))
  }

  override protected def extendedCheckRules(analyzer: Analyzer): Seq[LogicalPlan => Unit] = Nil

  override protected def resolutionRules(analyzer: Analyzer): List[Rule[LogicalPlan]] = Nil

  override protected def optimizerEarlyBatches: List[ExtendableOptimizerBatch] = Nil

  override protected def optimizerMainBatchRules: List[Rule[LogicalPlan]] = Nil

  override protected def optimizerPostBatches: List[ExtendableOptimizerBatch] = Nil

  override protected def strategies(planner: ExtendedPlanner): List[Strategy] = Nil

  /** Default copied from Spark code base. */
  override protected def extendedParserDialect: ParserDialect =
    try {
      val clazz = Utils.classForName(dialectClassName)
      clazz.newInstance().asInstanceOf[ParserDialect]
    } catch {
      case NonFatal(e) =>
        // Since we didn't find the available SQL Dialect, it will fail even for SET command:
        // SET spark.sql.dialect=sql; Let's reset as default dialect automatically.
        val dialect = conf.dialect
        // reset the sql dialect
        conf.unsetConf(SQLConf.DIALECT)
        // throw out the exception, and the default sql dialect will take effect for next query.
        throw new DialectException(
          s"""
              |Instantiating dialect '$dialect' failed.
              |Reverting to default dialect '${conf.dialect}'""".stripMargin, e)
    }

  // (suggestion) make this implicit to FunctionRegistry.
  protected def registerBuiltins(registry: FunctionRegistry): Unit = {
    FunctionRegistry.expressions.foreach {
      case (name, (info, builder)) => registry.registerFunction(name, builder)
    }
  }

  override protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser =
    new DDLParser(sqlParser.parse(_))

  override protected def registerFunctions(registry: FunctionRegistry): Unit = { }

}
