package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.DatasourceCatalog
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * The execution of ''DESCRIBE TABLES ... USING '' in the data source.
  *
  * Example of the resulting relation:
  * =====================================
  * |TABLE_NAME|DDL_STMT                |
  * =====================================
  * |Table1    |CREATE TABLE Table1 ... |
  * -------------------------------------
  * |View1     |CREATE VIEW View1 AS ...|
  * -------------------------------------
  *
  * if the data source's catalog does not have the relation then an
  * empty table will be returned.
  *
  * @param name The (qualified) name of the relation.
  * @param provider The data source class identifier.
  * @param options The options map.
  */
private[sql]
case class DescribeTableUsingRunnableCommand(name: Seq[String],
                                             provider: String, options: Map[String, String])
  extends LogicalPlan
    with RunnableCommand {

  override def output: Seq[Attribute] = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
    StructField("DDL_STMT", StringType, nullable = false) ::
    Nil
  ).toAttributes

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    dataSource match {
      case describableRelation: DatasourceCatalog =>
        Seq(describableRelation
          .getRelation(sqlContext, name, new CaseInsensitiveMap(options)) match {
            case None => Row("", "")
            case Some(describableRelation.RelationInfo(relName, _, _, ddl)) => Row(
              relName, ddl.getOrElse(""))
        })
      case _ =>
        throw new RuntimeException(s"The provided data source $provider does not support" +
          "describing its relations.")
    }
  }
}
