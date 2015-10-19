package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.ProviderUtils._
import org.apache.spark.sql.sources.DatasourceCatalog
import org.apache.spark.sql.types._

/**
 * Extracts all the table from the catalog
 */
case class ShowDataSourceTablesRunnableCommand(provider: String,
                                               options: Map[String, String])
  extends RunnableCommand {

  // The result of SHOW TABLES has one column: tableName
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("tableName", StringType, false) :: Nil)
    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {

    // try to instantiate
    val source: DatasourceCatalog = instantiateProvider(provider, "show datasource tables action")

    val rows = source.getTableNames(sqlContext, options).map {
      s => Row(s)
    }
    rows
  }
}
