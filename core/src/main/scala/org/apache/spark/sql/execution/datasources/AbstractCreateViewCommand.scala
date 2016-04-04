package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, Persisted}
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.sources.{AbstractViewProvider, CreateViewInput}
import org.apache.spark.sql.{DatasourceResolver, DefaultDatasourceResolver, Row, SQLContext}

import scala.reflect.ClassTag

/**
  * A command to create a view in both the spark catalog and a datasource.
  * @param view The view.
  * @param identifier The name of the view.
  * @param provider The package of the provider.
  * @param options The options of the command.
  * @param allowExisting True if this should not fail if the view already exists,
  *                      false otherwise
  * @tparam A The type of the view.
  */
case class CreatePersistentViewCommand[A <: AbstractView with Persisted: ClassTag](
    view: A,
    identifier: TableIdentifier,
    provider: String,
    options: Map[String, String],
    allowExisting: Boolean)
  extends AbstractCreateViewCommand[A]
  with Persisting[A] {

  val tag = implicitly[ClassTag[A]]

  def execute(sqlContext: SQLContext)(implicit resolver: DatasourceResolver): Seq[Row] =
    withValidProvider { provider =>
      ensureAllowedToWrite(sqlContext)
      registerInProvider(sqlContext, provider)
      registerInCatalog(sqlContext)
      Seq.empty
    }

  override def run(sqlContext: SQLContext): Seq[Row] =
    execute(sqlContext)(DefaultDatasourceResolver)
}

/**
  * A command to create a view that resides in spark catalog only.
  * @param view The view to create.
  * @param identifier The identifier of the view.
  * @param temporary Flag whether the creation was specified as temporary.
  * @tparam A The type of the view.
  */
case class CreateNonPersistentViewCommand[A <: AbstractView](
    view: A,
    identifier: TableIdentifier,
    temporary: Boolean)
  extends AbstractCreateViewCommand[A]
  with NonPersisting[A] {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    ensureAllowedToWrite(sqlContext)
    emitWarningIfNecessary()
    registerInCatalog(sqlContext)
    Seq.empty
  }
}


/**
  * A base trait for commands that create views.
  * @tparam A The type of the view.
  */
trait AbstractCreateViewCommand[A <: AbstractView] extends AbstractViewCommand[A] {
  val view: A

  /**
    * Checks if [[allowedToWriteRelationInSpark]] is true, otherwise throws
    * a [[RuntimeException]] that the relation already exists.
    * @param sqlContext The sqlContext.
    */
  def ensureAllowedToWrite(sqlContext: SQLContext): Unit = {
    if (!allowedToWriteRelationInSpark(sqlContext)) {
      sys.error(s"Relation ${identifier.table} already exists")
    }
  }

  /**
    * Determines if it is okay to write the relation in spark.
    * @param sqlContext The sqlContext.
    * @return True if it is okay to write in the spark catalog, false otherwise.
    */
  def allowedToWriteRelationInSpark(sqlContext: SQLContext): Boolean = {
    !sqlContext.catalog.tableExists(identifier.toSeq)
  }

  /**
    * Registers the view in the catalog.
    * @param sqlContext The sqlContext in which's catalog the view is registered.
    */
  def registerInCatalog(sqlContext: SQLContext): Unit = {
    sqlContext.registerRawPlan(view.plan, identifier.table)
  }
}

/**
  * A view creation command that also persists into a datasource.
  * @tparam A The type of the view the provider should be able to handle.
  */
trait Persisting[A <: AbstractView with Persisted] extends ProviderBound[A] {
  self: AbstractCreateViewCommand[A] =>

  val allowExisting: Boolean

  override def allowedToWriteRelationInSpark(sqlContext: SQLContext): Boolean = {
    allowExisting || !sqlContext.catalog.tableExists(identifier.toSeq)
  }

  /**
    * Registers the view in the provider.
    * @param sqlContext The sqlContext.
    * @param viewProvider The provider to register the view in.
    */
  def registerInProvider(sqlContext: SQLContext, viewProvider: AbstractViewProvider[A]): Unit = {
    viewProvider.create(CreateViewInput(sqlContext, options, identifier, view, allowExisting))
  }
}

/**
  * A view creation command that only persists the command into the spark catalog.
  * @tparam A The type of the view.
  */
trait NonPersisting[A <: AbstractView] {
  self: AbstractCreateViewCommand[A] =>

  /** Flag whether the creation was specified as temporary. */
  val temporary: Boolean

  /**
    * Emits a warning if the view creation was specified as temporary.
    */
  def emitWarningIfNecessary(): Unit = {
    if (!temporary) {
      log.warn(s"The relation: ${identifier.table} will be temporary although it is " +
        s"marked as non-temporary! in order to create a persistent view please use: " +
        s"'CREATE DIMENSION VIEW ... USING' syntax")
    }
  }
}

