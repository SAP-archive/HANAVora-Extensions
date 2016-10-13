package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.CaseSensitivityUtils._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.sources.RegisterAllTableRelations
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}

/**
  * Registers a table (i.e. REGISTER TABLE). In order to use this, a data source
  * needs to implement [[RegisterAllTableRelations]].
  *
  * @param tableName Table name.
  * @param provider Data source.
  * @param options Options.
  * @param ignoreConflicts If true, the conflicting table will be overwritten.
  * @param allowExisting If true, the existing table will be skipped and _not_ overwritten.
  */
private[sql] case class RegisterTableCommand(
    tableName: String,
    provider: String,
    options: Map[String, String],
    ignoreConflicts: Boolean,
    allowExisting: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    /**
      * Before executing, we check whether there is already a relation with that
      * name in the catalog.
      */
    val tableExists = sqlContext.catalog.tableExists(TableIdentifier(tableName))

    if (tableExists && !ignoreConflicts && !allowExisting) {
      sys.error(s"Relation $tableName already exists in Spark catalog.")
    } else if (tableExists && allowExisting) {
      logInfo(s"Table '$tableName' already exists and IF NOT EXISTS was specified. Skipping...")
    } else {
      /** Instantiate the provider */
      val resolver = DatasourceResolver.resolverFor(sqlContext)
      val resolvedProvider = resolver.newInstanceOfTyped[RegisterAllTableRelations](provider)

      /** Get the relation from the provider */
      val relation = resolvedProvider.getTableRelation(tableName, sqlContext, options)

      relation match {
        case None =>
          sys.error(s"Relation $tableName was not found in the catalog.")
        case Some(r) =>
          val lp = r.logicalPlan(sqlContext)
          if (lp.resolved) {
            sqlContext.validatedSchema(lp.schema).recover {
              case d: DuplicateFieldsException =>
                throw new RuntimeException(
                  s"Provider '$provider' returned a relation that has duplicate fields.",
                  d)
            }.get
          } else {
            // TODO(AC): With the new view interface, this can be checked
            logWarning(s"Adding relation $tableName with potentially unreachable fields.")
          }
          sqlContext.registerRawPlan(lp, tableName)
      }
    }
    Seq.empty
  }
}
