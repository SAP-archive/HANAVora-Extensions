package org.apache.spark.sql.sources.commands

import org.apache.spark.sql._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.parser.SapParserException

/**
 * Returned for "USE xyz" statements.
 *
 * Currently used to ignore any such statements
 * if "sql.ignore_use_statments=true",
 * else, an exception is thrown.
 *
 * @param input The "USE xyz" input statement
 */
private[sql] case class UseStatementCommand(input: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val confValue = sqlContext.sparkContext
      .getConf.getBoolean(CommonSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS,
      defaultValue = false)
    val sqlConfValue = sqlContext
      .getConf(CommonSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS,
        defaultValue = confValue.toString)
      .toBoolean
    if (sqlConfValue) {
      log.info(s"Ignoring statement:\n$input")
    } else {
      throw new SapParserException(input, 1, 1, "USE statement is not supported")
    }
    Nil
  }
}
