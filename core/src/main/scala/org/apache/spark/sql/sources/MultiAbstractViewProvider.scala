package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.plans.logical.{PersistedCubeView, PersistedDimensionView, PersistedView}

sealed trait MultiAbstractViewProvider

/**
  * An interface that marks a data source as a cube view provider.
  * It allows the user to push down views to the target data source where the cube
  * view's logical plan is serialized in the data source catalog.
  *
  * TODO (YH) we should discuss whether we want to support updating the view.
  */
trait CubeViewProvider extends MultiAbstractViewProvider {

  /**
    * Save the view in the catalog of the data source.
    *
    * @param createViewInput The parameters for the view creation.
    */
  def createCubeView(createViewInput: CreateViewInput[PersistedCubeView]): Unit


  /**
    * Drops the cube view from the catalog of the data source.
    *
    * @param dropViewInput The parameters to drop a view.
    */
  def dropCubeView(dropViewInput: DropViewInput): Unit

  private[sql] def toSingleCubeViewProvider: AbstractViewProvider[PersistedCubeView] = {
    new BaseAbstractViewProvider[PersistedCubeView] {
      override def drop(dropViewInput: DropViewInput): Unit = dropCubeView(dropViewInput)

      override def create(createViewInput: CreateViewInput[PersistedCubeView]): Unit =
        createCubeView(createViewInput)
    }
  }
}

/**
  * An interface that marks a data source as a dimension view provider.
  * It allows the user to push down views to the target data source where the dimension
  * view's logical plan is serialized in the data source catalog.
  *
  * TODO (YH) we should discuss whether we want to support updating the view.
  */
trait DimensionViewProvider extends MultiAbstractViewProvider {

  /**
    * Save the view in the catalog of the data source.
    *
    * @param createViewInput The parameters for the view creation.
    */
  def createDimensionView(createViewInput: CreateViewInput[PersistedDimensionView]): Unit


  /**
    * Drops the dimension view from the catalog of the data source.
    *
    * @param dropViewInput The parameters to drop the view.
    */
  def dropDimensionView(dropViewInput: DropViewInput): Unit

  private[sql] def toSingleDimensionViewProvider: AbstractViewProvider[PersistedDimensionView] = {
    new BaseAbstractViewProvider[PersistedDimensionView] {
      override def drop(dropViewInput: DropViewInput): Unit = {
        dropDimensionView(dropViewInput)
      }

      override def create(createViewInput: CreateViewInput[PersistedDimensionView]): Unit = {
        createDimensionView(createViewInput)
      }
    }
  }
}

/**
  * An interface that marks a data source as a view provider.
  * It allows the user to push down views to the target data source where the view's logical plan is
  * serialized in the data source catalog.
  *
  * TODO (YH) we should discuss whether we want to support updating the view.
  */
trait ViewProvider extends MultiAbstractViewProvider {

  /**
    * Save the view in the catalog of the data source.
    *
    * @param createViewInput The parameters to create the view.
    */
  def createView(createViewInput: CreateViewInput[PersistedView]): Unit


  /**
    * Drops the view from the catalog of the data source.
    *
    * @param dropViewInput The parameters to drop the view.
    */
  def dropView(dropViewInput: DropViewInput): Unit


  private[sql] def toSingleViewProvider: AbstractViewProvider[PersistedView] = {
    new BaseAbstractViewProvider[PersistedView] {
      override def drop(dropViewInput: DropViewInput): Unit = dropView(dropViewInput)

      override def create(createViewInput: CreateViewInput[PersistedView]): Unit = {
        createView(createViewInput)
      }
    }
  }
}

object MultiAbstractViewProvider {
  def unapply(arg: MultiAbstractViewProvider): Option[AbstractViewProvider[_]] = Some(
    arg match {
      case c: CubeViewProvider => c.toSingleCubeViewProvider
      case d: DimensionViewProvider => d.toSingleDimensionViewProvider
      case v: ViewProvider => v.toSingleViewProvider
    }
  )
}
