package org.apache.spark.sql.sources.commands.hive

import org.apache.spark.sql._

/**
  * An emulation for the Hive USE statement.
  *
  * If [[SapSQLConf.HIVE_EMULATION]] or [[CommonSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS]]
  * is set to true, any incoming USE statement will be ignored. Otherwise, an exception
  * will be thrown.
  *
  * @param parts The parts of the USE statement
  */
private[sql] case class UseCommand(parts: Seq[String]) extends HiveRunnableCommand {

  override protected def commandName: String = s"USE ${parts.mkString(" ")}"

  override protected def isHiveEmulationEnabled(sqlContext: SQLContext): Boolean = {
    val confValue = sqlContext.sparkContext
      .getConf.getBoolean(CommonSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS,
      defaultValue = false)
    val sqlConfValue = sqlContext
      .getConf(CommonSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS,
        defaultValue = confValue.toString)
      .toBoolean

    sqlConfValue || super.isHiveEmulationEnabled(sqlContext)
  }

  override protected def errorMessage: String =
    s"""${super.errorMessage}
       |USE can also be enabled by setting the ${CommonSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS}
       |to 'true'.""".stripMargin

  override def execute(sqlContext: SQLContext): Seq[Row] = {
    log.info(s"Ignoring statement:\n${parts.mkString(" ")}")
    Nil
  }
}
