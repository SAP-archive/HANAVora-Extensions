package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.sql.ViewKind
import org.apache.spark.sql.sources.{DropRelation, TemporaryFlagRelation, ViewHandle}

/**
  * A logical plan of a view.
  */
trait AbstractView extends UnaryNode {
  val plan: LogicalPlan

  val kind: ViewKind

  override def child: LogicalPlan = plan

  override def output: Seq[Attribute] = plan.output
}

/**
  * A view that has some persistence in a datasource.
  */
trait Persisted extends TemporaryFlagRelation with DropRelation {
  self: AbstractView =>

  val handle: ViewHandle

  override def isTemporary(): Boolean = false

  /** @inheritdoc */
  override def dropTable(): Unit = handle.drop()
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
