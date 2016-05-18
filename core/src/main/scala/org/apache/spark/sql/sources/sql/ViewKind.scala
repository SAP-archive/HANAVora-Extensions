package org.apache.spark.sql.sources.sql

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sources.ViewHandle

import scala.reflect._

sealed trait ViewKind {
  type A <: AbstractView
  val classTag: ClassTag[A]

  def createNonPersisted(plan: LogicalPlan): A
  def createPersisted(plan: LogicalPlan, handle: ViewHandle): A with Persisted
}

sealed abstract class BaseViewKind[V <: AbstractView: ClassTag] extends ViewKind {
  type A = V
  val classTag = implicitly[ClassTag[V]]
}

object Plain extends BaseViewKind[View] {
  override def createNonPersisted(plan: LogicalPlan): View = NonPersistedView(plan)

  override def createPersisted(plan: LogicalPlan, handle: ViewHandle): View with Persisted =
    PersistedView(plan, handle)
}

object Dimension extends BaseViewKind[DimensionView] {
  override def createNonPersisted(plan: LogicalPlan): DimensionView =
    NonPersistedDimensionView(plan)

  override def createPersisted(plan: LogicalPlan, handle: ViewHandle)
    : DimensionView with Persisted =
    PersistedDimensionView(plan, handle)
}

object Cube extends BaseViewKind[CubeView] {
  override def createNonPersisted(plan: LogicalPlan): CubeView =
    NonPersistedCubeView(plan)

  override def createPersisted(plan: LogicalPlan, handle: ViewHandle): CubeView with Persisted =
    PersistedCubeView(plan, handle)
}

object ViewKind {
  def unapply(string: Option[String]): Option[ViewKind] = string match {
    case None => Some(Plain)
    case Some(str) => str match {
      case "dimension" => Some(Dimension)
      case "cube" => Some(Cube)
      case _ => None
    }
  }
}
