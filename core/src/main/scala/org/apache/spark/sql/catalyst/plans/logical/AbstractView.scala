package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.TemporaryFlagRelation

import scala.reflect.ClassTag

/**
  * A logical plan of a view.
  */
trait AbstractView extends LogicalPlan {
  val plan: LogicalPlan
}

abstract class AbstractViewBase extends LeafNode with NoOutput

abstract class AbstractTaggedViewBase[A <: AbstractView with Persisted: ClassTag]
  extends AbstractViewBase {
  val tag = implicitly[ClassTag[A]]
}

/**
  * A view that has some persistence in a datasource.
  */
trait Persisted extends TemporaryFlagRelation {
  self: AbstractView =>

  val tag: ClassTag[_ <: AbstractView with Persisted]

  override def isTemporary(): Boolean = false
}

/**
  * A view that only exists in the spark catalog.
  */
trait NonPersisted extends TemporaryFlagRelation {
  self: AbstractView =>
  override def isTemporary(): Boolean = true
}

object AbstractView {
  def unapply(arg: AbstractView): Option[LogicalPlan] = Some(arg.plan)
}

/**
  * A logical plan that has no output.
  */
trait NoOutput {
  self: LogicalPlan =>

  override def output: Seq[Attribute] = Nil
}
