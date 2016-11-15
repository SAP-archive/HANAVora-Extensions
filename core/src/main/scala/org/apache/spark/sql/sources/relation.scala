package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.view._
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Trait for marking something as a relation.
 *
 * Only [[BaseRelation]]s or [[LogicalPlan]]s should be extended with this.
 */
sealed trait Relation {

  def isTemporary: Boolean

  /**
    * The [[RelationKind]] of this [[Relation]].
    *
    * It is safe to say that each [[View]] returns the [[View]] relation kind
    * and each [[Table]] returns the [[Table]] relation kind since these methods
    * are defined finally on the subclasses.
    *
    * @return [[View]] if this is a [[View]], [[Table]] if this is a [[Table]].
    */
  def kind: RelationKind
}

object Relation {

  /**
    * Searches the plan for the first in-order [[Relation]] or [[LogicalRelation]]([[Relation]]).
    *
    * @param plan The [[LogicalPlan]] to search.
    * @return The optional found [[Relation]].
    */
  def unapply(plan: LogicalPlan): Option[Relation] =
    plan.collectFirst {
      case r: Relation => r
      case LogicalRelation(r: Relation, _) => r
    }
}

/**
  * A relation that is a table.
  */
trait Table extends Relation {

  /** @inheritdoc */
  override final def kind: RelationKind = Table
}

/**
  * A relation that is a view.
  */
trait View extends Relation {
  this: ViewSpecification =>
}

/** A specification of a [[View]] [[Relation]] that states the view type. */
sealed trait ViewSpecification extends Product with ClassNameAsName {

  /** The [[ViewKind]] of this [[View]]] relation. */
  def kind: ViewKind
}

/** A plain (i.e. default) view. */
trait Plain extends ViewSpecification {

  /** @inheritdoc */
  override def kind: ViewKind = PlainViewKind
}

/** A cube view. */
trait Cube extends ViewSpecification {

  /** @inheritdoc */
  override def kind: ViewKind = CubeViewKind
}

/** A dimension view. */
trait Dimension extends ViewSpecification {

  /** @inheritdoc */
  override def kind: ViewKind = DimensionViewKind
}

/**
  * A kind of a relation.
  */
sealed trait RelationKind extends Product {

  /**
    * @return The name of the [[RelationKind]]
    */
  def name: String
}

object RelationKind {

  /**
    * Returns the [[RelationKind]] of the first [[Relation]] in the plan.
    *
    * @param plan The [[LogicalPlan]] to search.
    * @return [[Some]] of the [[RelationKind]] of the first [[Relation]] in the plan,
    *         otherwise [[None]]
    */
  def kindOf(plan: LogicalPlan): Option[RelationKind] =
    Relation.unapply(plan).map(_.kind)
}

sealed trait ClassNameAsName extends RelationKind {
  def name: String = productPrefix
}

/** The table [[RelationKind]] */
case object Table extends RelationKind with ClassNameAsName

/** A relation that is a view. */
sealed trait ViewKind extends RelationKind {
  type A <: AbstractView

  /**
    * Creates a view of type [[A]].
    *
    * @param plan The plan to wrap in the created view.
    * @return A view of type [[A]] wrapping the given [[LogicalPlan]].
    */
  def createNonPersisted(plan: LogicalPlan): A

  /**
    * Creates a view of type [[A]] that is [[Persisted]].
    *
    * @param plan The plan to wrap in the created view.
    * @return A view of type [[A]] wrapping the given [[LogicalPlan]].
    */
  def createPersisted(plan: LogicalPlan, handle: ViewHandle, provider: String): A with Persisted
}

/** A plain view kind (i.e. default view) */
case object PlainViewKind extends ViewKind {
  type A = PlainView

  /** @inheritdoc */
  override def createNonPersisted(plan: LogicalPlan): PlainView = NonPersistedPlainView(plan)

  /** @inheritdoc */
  override def createPersisted(plan: LogicalPlan,
                               handle: ViewHandle,
                               provider: String): PlainView with Persisted =
    PersistedPlainView(plan, handle, provider)

  /** @inheritdoc */
  override val name: String = "View"
}

/** A dimension view kind. */
case object DimensionViewKind extends ViewKind {
  type A = DimensionView

  /** @inheritdoc */
  override def createNonPersisted(plan: LogicalPlan): DimensionView =
    NonPersistedDimensionView(plan)

  /** @inheritdoc */
  override def createPersisted(plan: LogicalPlan,
                               handle: ViewHandle,
                               provider: String): DimensionView with Persisted =
    PersistedDimensionView(plan, handle, provider)

  /** @inheritdoc */
  override val name: String = "Dimension"
}

/** A cube view kind. */
case object CubeViewKind extends ViewKind with ClassNameAsName {
  type A = CubeView

  /** @inheritdoc */
  override def createNonPersisted(plan: LogicalPlan): CubeView =
    NonPersistedCubeView(plan)

  /** @inheritdoc */
  override def createPersisted(plan: LogicalPlan,
                               handle: ViewHandle,
                               provider: String): CubeView with Persisted =
    PersistedCubeView(plan, handle, provider)

  /** @inheritdoc */
  override val name: String = "Cube"
}

object ViewKind {
  def unapply(string: Option[String]): Option[ViewKind] = string match {
    case None => Some(PlainViewKind)
    case Some(str) => str match {
      case "dimension" => Some(DimensionViewKind)
      case "cube" => Some(CubeViewKind)
      case _ => None
    }
  }
}

/** The collection [[RelationKind]] */
case object Collection extends RelationKind with ClassNameAsName

/** The graph [[RelationKind]] */
case object Graph extends RelationKind with ClassNameAsName
