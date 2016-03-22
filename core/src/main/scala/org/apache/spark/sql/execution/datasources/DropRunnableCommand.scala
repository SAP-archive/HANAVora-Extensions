package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.DropRelation
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Execution for DROP TABLE command.
  *
  * @see [[DropRelation]]
  * @param allowNotExisting IF NOT EXISTS.
  * @param tableIdentifier Identifier of the relation to drop
  * @param cascade CASCADE.
  */
private[sql] case class DropRunnableCommand(
    allowNotExisting: Boolean,
    tableIdentifier: TableIdentifier,
    cascade: Boolean,
    dropRelation: Option[DropRelation])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    if (sqlContext.catalog.tableExists(tableIdentifier.toSeq)) {
      executeDrop(sqlContext)
    } else if (allowNotExisting) {
      Seq.empty
    } else {
      sys.error("Could not find any matching tables to drop.")
    }
  }

  private def executeDrop(sqlContext: SQLContext): Seq[Row] = {
    val references = getReferencingRelations(sqlContext)
    (references.size, cascade) match {
      case (n, false) if n > 1 =>
        sys.error("Can not drop because more than one relation has " +
          s"references to the target relation: ${references.mkString(",")}. " +
          s"to force drop use 'CASCADE'.")
      case _ =>
        references.foreach(sqlContext.dropTempTable)
        dropRelation.foreach(_.dropTable())
    }

    Seq.empty
  }

  private def getReferencingRelations(sqlContext: SQLContext): Set[String] = {
    val catalog = sqlContext.catalog
    val tables = sqlContext.tableNames()
                           .map(name => (name, catalog.lookupRelation(Seq(name))))
                           .toMap

    def inner(acc: Set[String], next: List[String]): Set[String] = {
      def hasReference(plan: LogicalPlan): Boolean = plan.find {
        case UnresolvedRelation(Seq(ident), _) if acc.contains(ident) =>
          true
        case _ => false
      }.isDefined

      next match {
        case Nil => acc

        case head :: tail =>
          val newReferences = tables.collect {
            case (name, plan) if !acc.contains(name) && hasReference(plan) =>
              name
          }
          inner(acc ++ newReferences, tail ++ newReferences)
      }
    }

    inner(Set(tableIdentifier.table), tableIdentifier.table :: Nil)
  }
}
