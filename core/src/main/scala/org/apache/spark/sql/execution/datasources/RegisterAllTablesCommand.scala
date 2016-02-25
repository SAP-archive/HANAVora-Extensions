package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.execution.ProviderUtils._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.RegisterAllTableRelations
import org.apache.spark.sql.{Row, SQLContext}
import SqlContextAccessor._

/**
  * Provides execution for REGISTER ALL TABLES statements. A data source
  * needs to implement [[RegisterAllTableRelations]] to be able to use this
  * command with them.
  *
  * @param provider Data source.
  * @param options options.
  * @param ignoreConflicts If true, conflicting tables will be ignored.
  */
private[sql] case class RegisterAllTablesCommand(
    provider: String,
    options: Map[String, String],
    ignoreConflicts: Boolean)
  extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {

    /** Provider instantiation. */
    val resolvedProvider: RegisterAllTableRelations =
      instantiateProvider(provider, "register all tables action")

    /** Get all relations known to the provider with a given set of options. */
    val relations = resolvedProvider.getAllTableRelations(sqlContext, options)

    /** Partition relations in two groups: new and already existing */
    val (existingRelations, newRelations) = relations
      .partition({
        case (name, relation) => sqlContext.catalog.tableExists(name :: Nil)
      })

    /** If [[ignoreConflicts]] is false, throw if there are existing relations */
    if (!ignoreConflicts && existingRelations.nonEmpty) {
      sys.error(s"Some tables already exists: ${existingRelations.keys.mkString(", ")}")
    }

    /** Register new relations */
    newRelations.foreach({
      case (name, source) =>
        val lp = source.logicalPlan(sqlContext)
        sqlContext.registerRawPlan(lp, name)
    })

    // XXX: This could return the list of registered relations
    Seq.empty
  }
}
