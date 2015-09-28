package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.execution.ProviderUtils._
import org.apache.spark.sql.sources.{LogicalRelation, RegisterAllTableRelations}
import org.apache.spark.sql.{DataFrame, SQLContext}

case class RegisterTableCommand(
    tableName: String,
    provider: String,
    options: Map[String, String],
    ignoreConflicts: Boolean)
  extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    val resolvedProvider: RegisterAllTableRelations =
      instantiateProvider(provider, "register table action")
    val relation = resolvedProvider.getTableRelation(tableName, sqlContext, options)
    relation match {
      case None => sys.error("Table $tableName is not found in the catalog.")
      case Some(r) =>
        val existsInSpark = sqlContext.catalog.tableExists(tableName :: Nil)
        sqlContext.catalog.tableExists(tableName :: Nil)
        if (!ignoreConflicts && existsInSpark) {
          sys.error(s"Table $tableName already exists is Spark catalog.")
        }
        sqlContext.registerDataFrameAsTable(DataFrame(sqlContext, LogicalRelation(r)), tableName)
        Seq.empty
    }
  }
}
