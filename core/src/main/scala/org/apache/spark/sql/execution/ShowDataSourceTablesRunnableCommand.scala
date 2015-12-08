package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.ProviderUtils._
import org.apache.spark.sql.sources.DatasourceCatalog
import org.apache.spark.sql.types._

/**
  * Extracts all the table from the catalog. This is  used
  * to execute SHOW TABLES statements.
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
    val source: DatasourceCatalog = instantiateProvider(provider, "show datasource tables action")
    val tableNames = source.getTableNames(sqlContext, options)
    val rows = tableNames.map({ case tableName => Row(tableName) })
    rows
  }
}
