package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.CaseSensitivityUtils._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.sources.{LogicalPlanSource, RegisterAllTableRelations}
import org.apache.spark.sql.util.CollectionUtils._
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}

/**
  * Provides execution for REGISTER ALL TABLES statements. A data source
  * needs to implement [[RegisterAllTableRelations]] to be able to use this
  * command with them.
  *
  * @param provider Data source.
  * @param options options.
  * @param ignoreConflicts If true, conflicting tables will be overwritten.
  * @param allowExisting If true, existing tables will be skipped and _not_ overwritten.
  */
private[sql] case class RegisterAllTablesCommand(
    provider: String,
    options: Map[String, String],
    ignoreConflicts: Boolean,
    allowExisting: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    /** Provider instantiation. */
    val resolver = DatasourceResolver.resolverFor(sqlContext)
    val resolvedProvider = resolver.newInstanceOfTyped[RegisterAllTableRelations](provider)

    /** Get all relations known to the provider with a given set of options. */
    val relations = resolvedProvider.getAllTableRelations(sqlContext, options)

    /** Partition relations in two groups: new and already existing */
    val (existingRelations, newRelations) = relations
      .partition({
        case (name, relation) => sqlContext.catalog.tableExists(new TableIdentifier(name))
      })

    val duplicateNames = relations.keys.toList.map(sqlContext.fixCase).duplicates

    val tableExists = existingRelations.nonEmpty || duplicateNames.nonEmpty

    /** If [[tableExists]] is true [[allowExisting]] is false and [[ignoreConflicts]] is false,
      * throw if there are existing relations */
    if (tableExists && !allowExisting && !ignoreConflicts) {
      sys.error(Seq(
        existingRelations.nonEmptyOpt.map { existing =>
          s"Some tables already exists: ${existingRelations.keys.mkString(", ")}"
        },
        duplicateNames.nonEmptyOpt.map { duplicates =>
          s"Duplicate relation name(s): ${duplicates.mkString(",")}"
        }
      ).flatten.mkString("There were some errors: ", "\n", ""))
    } else if (tableExists && allowExisting) {
      registerRelations(sqlContext, newRelations)
    } else {
      registerRelations(sqlContext, relations)
    }

    // TODO: This could return the list of registered relations
    Seq.empty
  }

  private def registerRelations(sqlContext: SQLContext,
                                relations: Map[String, LogicalPlanSource]): Unit = {
    /** Register relations */
    relations.map {
      case (name, source) =>
        val lp = source.logicalPlan(sqlContext)
        if (lp.resolved) {
          sqlContext.validatedSchema(lp.schema).recover {
            case d: DuplicateFieldsException =>
              throw new RuntimeException(
                s"Provider '$provider' returned a relation that has duplicate fields.",
                d)
          }.get
        } else {
          // TODO(AC): With the new view interface, this can be checked
          logWarning(s"Adding relation $name with potentially unreachable fields.")
        }
        name -> lp
    }.foreach {
      case (name, plan) =>
        sqlContext.registerRawPlan(plan, name)
    }
  }
}
