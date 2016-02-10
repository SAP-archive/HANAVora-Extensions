package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.ProviderUtils._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.DatasourceCatalog
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Extracts all the table from the catalog. This is  used
  * to execute SHOW TABLES statements.
  *
  * @deprecated (YH) consider using [[ShowTablesUsingRunnableCommand]] instead.
  */
private[sql] case class ShowDataSourceTablesRunnableCommand(
    provider: String,
    options: Map[String, String])
  extends RunnableCommand {

  /**
    * The output of SHOW TABLES is a single columns: the table name.
    */
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("tableName", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    log.warn("the 'SHOW DATASOURCETABLES statement is deprecated. Please use the more standard" +
      " 'SHOW TABLES ... USING' statement to get persisted relations from a data source catalog.")
    val source: DatasourceCatalog = instantiateProvider(provider, "show datasource tables action")
    val tableNames = source.getTableNames(sqlContext, options)
    val rows = tableNames.map({ case tableName => Row(tableName) })
    rows
  }
}
