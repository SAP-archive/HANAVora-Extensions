package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.execution.ProviderUtils._
import org.apache.spark.sql.sources.{LogicalRelation, RegisterAllTableRelations}
import org.apache.spark.sql.{DataFrame, SQLContext}

case class RegisterAllTablesCommand(
                                     provider: String,
                                     options: Map[String, String],
                                     ignoreConflicts: Boolean
                                     ) extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    val resolvedProvider: RegisterAllTableRelations =
      instantiateProvider(provider, "register all tables action")
    val relations = resolvedProvider.getAllTableRelations(sqlContext, options)

    val existingTables = relations.flatMap({ case (name, _) =>
      if (sqlContext.catalog.tableExists(name :: Nil)) {
        Some(name)
      } else {
        None
      }
    }).toSeq

    if (!ignoreConflicts && existingTables.nonEmpty) {
      sys.error(s"Some tables already exists: ${existingTables.mkString(", ")}")
    }

    relations
      .filter({
      case (name, _) => !existingTables.contains(name)
    })
      .foreach({
      case (name, relation) =>
        sqlContext.registerDataFrameAsTable(
          DataFrame(sqlContext, LogicalRelation(relation)), name)
    })
    Seq.empty
  }
}
