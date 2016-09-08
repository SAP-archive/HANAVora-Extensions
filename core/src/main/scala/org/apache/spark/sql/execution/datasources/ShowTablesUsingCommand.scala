package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.DatasourceCatalog
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * The execution of ''SHOW TABLES ... USING '' in the data source.
  *
  * Example of the resulting relation:
  * ================================
  * |TABLE_NAME|IS_TEMPORARY|KIND  |
  * ================================
  * |Table1    |TRUE        |TABLE |
  * --------------------------------
  * |View1     |FALSE       |VIEW  |
  * --------------------------------
  *
  * @param provider The data source class identifier.
  * @param options The options map.
  */
private[sql]
case class ShowTablesUsingCommand(provider: String, options: Map[String, String])
  extends LogicalPlan
  with RunnableCommand {

  override def output: Seq[Attribute] = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
    StructField("IS_TEMPORARY", StringType, nullable = false) ::
    StructField("KIND", StringType, nullable = false) ::
    Nil
  ).toAttributes

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dataSource: Any = DatasourceResolver.resolverFor(sqlContext).newInstanceOf(provider)

    dataSource match {
      case describableRelation: DatasourceCatalog =>
        describableRelation
          .getRelations(sqlContext, new CaseInsensitiveMap(options))
          .map(relationInfo => Row(
            relationInfo.name,
            relationInfo.isTemporary.toString.toUpperCase,
            relationInfo.kind.toUpperCase))
      case _ =>
        throw new RuntimeException(s"The provided data source $provider does not support " +
        "showing its relations.")
    }
  }
}
