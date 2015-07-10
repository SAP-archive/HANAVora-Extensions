package org.apache.spark.sql.execution

import org.apache.spark.sql.sources.DatasourceCatalog
import org.apache.spark.sql.types._
import org.apache.spark.sql.{VelocitySQLContext, SQLContext}
import org.apache.spark.sql.catalyst.expressions._

/**
 * Extracts all the table from the catalog
 */
case class ShowDataSourceTablesRunnableCommand(classIdentifier: String,
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
    val source = instantiateDatasourceCatalogClass(classIdentifier)

    val rows = source.getTableNames(sqlContext,options).map {
      s => Row(s)
    }
    rows
  }

  private def instantiateDatasourceCatalogClass(name : String) : DatasourceCatalog = {
    try {
      Class.forName(classIdentifier).newInstance().asInstanceOf[DatasourceCatalog]
    } catch {
      case cnf : ClassNotFoundException  =>
        try {
          Class.forName(classIdentifier + ".DefaultSource").newInstance()
            .asInstanceOf[DatasourceCatalog]
        } catch {
          case e => throw
            new ClassNotFoundException(s"""Cannot instantiate $name.DefaultSource""",e)
        }
      case e => throw new ClassNotFoundException(s"""Cannot instantiate $name""", e)
    }
  }
}
