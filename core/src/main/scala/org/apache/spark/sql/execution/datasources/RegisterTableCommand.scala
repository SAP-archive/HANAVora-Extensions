package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.ProviderUtils._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.{BaseRelationSource, RegisterAllTableRelations}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import SqlContextAccessor._

/**
  * Registers a table (i.e. REGISTER TABLE). In order to use this, a data source
  * needs to implement [[RegisterAllTableRelations]].
  *
  * @param tableName Table name.
  * @param provider Data source.
  * @param options Options.
  * @param ignoreConflicts If true, no exception is thrown if the table is already registered.
  */
private[sql] case class RegisterTableCommand(
    tableName: String,
    provider: String,
    options: Map[String, String],
    ignoreConflicts: Boolean)
  extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {

    /** Instantiate the provider */
    val resolvedProvider: RegisterAllTableRelations =
      instantiateProvider(provider, "register table action")

    /** Get the relation from the provider */
    val relation = resolvedProvider.getTableRelation(tableName, sqlContext, options)

    relation match {
      case None =>
        sys.error(s"Table $tableName is not found in the catalog.")
      case Some(r) if !ignoreConflicts && sqlContext
        .catalog.tableExists(new TableIdentifier(tableName)) =>
        sys.error(s"Table $tableName already exists is Spark catalog.")
      case Some(r) =>
        val lp = r.logicalPlan(sqlContext)
        sqlContext.registerRawPlan(lp, tableName)
        Seq.empty
    }
  }
}
