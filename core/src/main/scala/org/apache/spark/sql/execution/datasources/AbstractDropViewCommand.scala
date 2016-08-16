package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.sources.sql.ViewKind
import org.apache.spark.sql.sources.{AbstractViewProvider, DropViewInput}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * A command to drop a view.
  */
trait AbstractDropViewCommand extends AbstractViewCommand {
  /**
    * Drops the view referenced by this from the spark catalog, iff present.
    * @param sqlContext The sqlContext
    */
  def dropFromSpark(sqlContext: SQLContext): Unit = {
    val catalog = sqlContext.catalog
    val relationId = alterByCatalystSettings(catalog, identifier)

    if (catalog.tableExists(relationId)) {
      catalog.unregisterTable(relationId)
    }
  }
}

/**
  * A drop command that also deletes the view from some persistence source.
  */
trait UnPersisting extends ProviderBound {
  self: AbstractDropViewCommand =>

  /** Should this fail if the view to drop does not exist. */
  val allowNotExisting: Boolean

  /**
    * Executes the drop on the underlying provider.
    * @param sqlContext The sqlContext
    * @param viewProvider The provider to execute the drop on.
    */
  def dropFromProvider(sqlContext: SQLContext, viewProvider: AbstractViewProvider[_]): Unit = {
    val relationId = alterByCatalystSettings(sqlContext.catalog, identifier)
    viewProvider.drop(DropViewInput(sqlContext, options, relationId, allowNotExisting))
  }
}

/**
  * Command to drop views.
  * @param identifier The identifier of the view to drop.
  * @param provider The provider package.
  * @param options The options of the command.
  * @param allowNotExisting False if the command should fail when the
  *                         view does not exist, true otherwise.
  */
case class DropPersistentViewCommand(
    kind: ViewKind,
    identifier: TableIdentifier,
    provider: String,
    options: Map[String, String],
    allowNotExisting: Boolean)
  extends AbstractDropViewCommand
  with UnPersisting {

  override def run(sqlContext: SQLContext): Seq[Row] =
    withValidProvider(sqlContext) { provider =>
      dropFromSpark(sqlContext)
      dropFromProvider(sqlContext, provider)
      Seq.empty
    }
}
