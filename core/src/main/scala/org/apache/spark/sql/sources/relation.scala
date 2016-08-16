package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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
  * A relation that is a view.
  */
trait View extends Relation {

  /** @inheritdoc */
  override final def kind: RelationKind = View
}

/**
  * A relation that is a table.
  */
trait Table extends Relation {

  /** @inheritdoc */
  override final def kind: RelationKind = Table
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
    * Returns the [[RelationKind]] of the first [[Relation]] in the plan or the default value
    *
    * @param plan The [[LogicalPlan]] to search.
    * @return The [[RelationKind]] of the first [[Relation]] in the plan or the default value.
    */
  def kindOf(plan: LogicalPlan, default: => RelationKind): RelationKind =
    Relation.unapply(plan).fold(default)(_.kind)
}

sealed trait ClassNameAsName extends RelationKind {
  def name: String = productPrefix
}

/** The table [[RelationKind]] */
case object Table extends RelationKind with ClassNameAsName

/** The view [[RelationKind]] */
case object View extends RelationKind with ClassNameAsName
