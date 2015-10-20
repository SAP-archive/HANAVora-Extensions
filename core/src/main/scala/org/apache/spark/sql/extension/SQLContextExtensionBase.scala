package org.apache.spark.sql.extension

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.errors.DialectException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.DDLParser
import org.apache.spark.util.Utils

import scala.util.control.NonFatal

@DeveloperApi
private[sql] trait SQLContextExtensionBase extends SQLContextExtension {
  self: SQLContext =>

  override protected def resolutionRules(analyzer: Analyzer): List[Rule[LogicalPlan]] = Nil

  override protected def optimizerRules: List[Rule[LogicalPlan]] = Nil

  override protected def strategies(planner: ExtendedPlanner): List[Strategy] = Nil

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

  override protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser =
    new DDLParser(sqlParser.parse(_))

  override protected def registerFunctions(registry: FunctionRegistry): Unit = { }

}
