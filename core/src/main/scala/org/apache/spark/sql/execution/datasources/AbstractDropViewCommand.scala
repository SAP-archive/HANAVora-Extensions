package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, Persisted}
import org.apache.spark.sql.sources.{AbstractViewProvider, DropViewInput}
import org.apache.spark.sql.{DatasourceResolver, DefaultDatasourceResolver, Row, SQLContext}

import scala.reflect.ClassTag

/**
  * A command to drop a view.
  * @tparam A The type of the view.
  */
trait AbstractDropViewCommand[A <: AbstractView] extends AbstractViewCommand[A] {
  /**
    * Drops the view referenced by this from the spark catalog, iff present.
    * @param sqlContext The sqlContext
    */
  def dropFromSpark(sqlContext: SQLContext): Unit = {
    val catalog = sqlContext.catalog

    if (catalog.tableExists(identifier)) {
      catalog.unregisterTable(identifier)
    }
  }
}

/**
  * A drop command that also deletes the view from some persistence source.
  * @tparam A The type of the view.
  */
trait UnPersisting[A <: AbstractView with Persisted] extends ProviderBound[A] {
  self: AbstractDropViewCommand[A] =>

  /** Should this fail if the view to drop does not exist. */
  val allowNotExisting: Boolean

  /**
    * Executes the drop on the underlying provider.
    * @param sqlContext The sqlContext
    * @param viewProvider The provider to execute the drop on.
    */
  def dropFromProvider(sqlContext: SQLContext, viewProvider: AbstractViewProvider[A]): Unit = {
    viewProvider.drop(DropViewInput(sqlContext, options, identifier, allowNotExisting))
  }
}

/**
  * Command to drop views.
  * @param identifier The identifier of the view to drop.
  * @param provider The provider package.
  * @param options The options of the command.
  * @param allowNotExisting False if the command should fail when the
  *                         view does not exist, true otherwise.
  * @tparam A The type of the view.
  */
case class DropPersistentViewCommand[A <: AbstractView with Persisted: ClassTag](
    identifier: TableIdentifier,
    provider: String,
    options: Map[String, String],
    allowNotExisting: Boolean)
  extends TaggedViewCommand[A]
  with AbstractDropViewCommand[A]
  with UnPersisting[A] {

  def execute(sqlContext: SQLContext)
             (implicit resolver: DatasourceResolver): Seq[Row] =
    withValidProvider { provider =>
      dropFromSpark(sqlContext)
      dropFromProvider(sqlContext, provider)
      Seq.empty
    }

  override def run(sqlContext: SQLContext): Seq[Row] =
    execute(sqlContext)(DefaultDatasourceResolver)
}
