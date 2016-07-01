package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, LogicalPlan}
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.sources.sql.ViewKind
import org.apache.spark.sql.sources.{AbstractViewProvider, CreateViewInput, ViewHandle}
import org.apache.spark.sql.{DatasourceResolver, DefaultDatasourceResolver, Row, SQLContext}

/**
  * A command to create a view in both the spark catalog and a datasource.
  * @param kind The kind of the view
  * @param plan The logical plan the view will wrap.
  * @param identifier The name of the view.
  * @param provider The package of the provider.
  * @param options The options of the command.
  * @param allowExisting True if this should not fail if the view already exists,
  *                      false otherwise
  */
case class CreatePersistentViewCommand(
    kind: ViewKind,
    identifier: TableIdentifier,
    plan: LogicalPlan,
    viewSql: String,
    provider: String,
    options: Map[String, String],
    allowExisting: Boolean)
  extends AbstractCreateViewCommand
  with Persisting {

  override def run(sqlContext: SQLContext): Seq[Row] =
    withValidProvider(sqlContext) { provider =>
      ensureAllowedToWrite(sqlContext)
      val handle = registerInProvider(sqlContext, provider)
      val view = kind.createPersisted(plan, handle, this.provider)
      registerInCatalog(view, sqlContext)
      Seq.empty
    }
}

/**
  * A command to create a view that resides in spark catalog only.
  * @param plan The logical plan the view will wrap.
  * @param identifier The identifier of the view.
  * @param temporary Flag whether the creation was specified as temporary.
  */
case class CreateNonPersistentViewCommand(
    kind: ViewKind,
    identifier: TableIdentifier,
    plan: LogicalPlan,
    temporary: Boolean)
  extends AbstractCreateViewCommand
  with NonPersisting {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    ensureAllowedToWrite(sqlContext)
    emitWarningIfNecessary()
    val view = kind.createNonPersisted(plan)
    registerInCatalog(view, sqlContext)
    Seq.empty
  }
}


/**
  * A base trait for commands that create views.
  */
trait AbstractCreateViewCommand extends AbstractViewCommand {
  val plan: LogicalPlan

  /**
    * Checks if [[allowedToWriteRelationInSpark]] is true, otherwise throws
    * a [[RuntimeException]] that the relation already exists.
    * @param sqlContext The sqlContext.
    */
  def ensureAllowedToWrite(sqlContext: SQLContext): Unit = {
    if (!allowedToWriteRelationInSpark(sqlContext)) {
      sys.error(s"Relation ${alterByCatalystSettings(sqlContext.catalog, identifier).table} " +
        s"already exists")
    }
  }

  /**
    * Determines if it is okay to write the relation in spark.
    * @param sqlContext The sqlContext.
    * @return True if it is okay to write in the spark catalog, false otherwise.
    */
  def allowedToWriteRelationInSpark(sqlContext: SQLContext): Boolean = {
    !sqlContext.catalog.tableExists(alterByCatalystSettings(sqlContext.catalog, identifier))
  }

  /**
    * Registers the view in the catalog.
    * @param sqlContext The sqlContext in which's catalog the view is registered.
    */
  def registerInCatalog(view: AbstractView, sqlContext: SQLContext): Unit = {
    sqlContext.registerRawPlan(view, alterByCatalystSettings(sqlContext.catalog, identifier).table)
  }
}

/**
  * A view creation command that also persists into a datasource.
  */
trait Persisting extends ProviderBound {
  self: AbstractCreateViewCommand =>

  /** The sql of the CREATE VIEW command */
  val viewSql: String

  val allowExisting: Boolean

  override def allowedToWriteRelationInSpark(sqlContext: SQLContext): Boolean = {
    allowExisting ||
      !sqlContext.catalog.tableExists(alterByCatalystSettings(sqlContext.catalog, identifier))
  }

  /**
    * Registers the view in the provider.
    * @param sqlContext The sqlContext.
    * @param viewProvider The provider to register the view in.
    */
  def registerInProvider(sqlContext: SQLContext,
                         viewProvider: AbstractViewProvider[_]): ViewHandle = {
    viewProvider.create(
      CreateViewInput(sqlContext, plan, viewSql, options,
        alterByCatalystSettings(sqlContext.catalog, identifier), allowExisting))
  }
}

/**
  * A view creation command that only persists the command into the spark catalog.
  */
trait NonPersisting {
  self: AbstractCreateViewCommand =>

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

