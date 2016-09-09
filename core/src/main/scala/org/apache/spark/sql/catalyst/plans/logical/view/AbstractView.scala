package org.apache.spark.sql.catalyst.plans.logical.view

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.commands.WithOrigin
import org.apache.spark.sql.sources.sql.{SqlLikeRelation, ViewKind}

/**
  * A logical plan of a view.
  */
trait AbstractView extends UnaryNode with sources.View {
  val plan: LogicalPlan

  val viewKind: ViewKind

  override def child: LogicalPlan = plan

  override def output: Seq[Attribute] = plan.output
}

/**
  * A view that has some persistence in a datasource.
  */
trait Persisted
  extends DropRelation
  with SqlLikeRelation
  with WithOrigin {

  this: AbstractView with Relation =>

  val handle: ViewHandle

  override val provider: String

  override def isTemporary: Boolean = false

  /** @inheritdoc */
  override def relationName: String = handle.name

  /** @inheritdoc */
  override def dropTable(): Unit = handle.drop()
}

/**
  * A view that only exists in the spark catalog.
  */
trait NonPersisted {
  self: AbstractView with Relation =>
  override def isTemporary: Boolean = true
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
