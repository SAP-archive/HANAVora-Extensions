package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.commands.{WithExplicitRelationKind, WithOrigin}
import org.apache.spark.sql.sources.sql.ViewKind

/**
  * A logical plan of a view.
  */
trait AbstractView extends UnaryNode with commands.View {
  val plan: LogicalPlan

  val kind: ViewKind

  override def child: LogicalPlan = plan

  override def output: Seq[Attribute] = plan.output
}

/**
  * A view that has some persistence in a datasource.
  */
trait Persisted
  extends TemporaryFlagRelation
  with DropRelation
  with WithOrigin {
  self: AbstractView with WithExplicitRelationKind =>

  val handle: ViewHandle

  override val provider: String

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
