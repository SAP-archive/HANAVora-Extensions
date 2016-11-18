package org.apache.spark.sql.sources.commands.hive

import org.apache.spark.sql.{Row, SQLContext, SapSQLConf}
import org.apache.spark.sql.execution.RunnableCommand

/**
  * A [[RunnableCommand]] that errors if there is no hive emulation enabled.
  *
  * Hive emulation itself can be enabled via [[SapSQLConf.HIVE_EMULATION]].
  */
trait HiveRunnableCommand extends RunnableCommand {

  protected def commandName: String

  override def run(sqlContext: SQLContext): Seq[Row] = {
    assertHiveEmulationEnabled(sqlContext)
    execute(sqlContext)
  }

  protected def assertHiveEmulationEnabled(sqlContext: SQLContext): Unit = {
    if (!isHiveEmulationEnabled(sqlContext)) {
      sys.error(errorMessage)
    }
  }

  protected def errorMessage: String =
    s"""$commandName is not supported without hive emulation.
      |To enable hive emulation, set
      |${SapSQLConf.HIVE_EMULATION.key} to 'true'.
      |The usage of this flag is experimental.""".stripMargin


  protected def isHiveEmulationEnabled(sqlContext: SQLContext): Boolean =
    sqlContext.getConf(SapSQLConf.HIVE_EMULATION)

  protected def execute(sqlContext: SQLContext): Seq[Row]
}
