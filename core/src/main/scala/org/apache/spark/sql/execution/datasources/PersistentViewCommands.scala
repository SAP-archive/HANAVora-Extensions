package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

/**
  * Used to persist a given view in a given data source's catalog.
  *
  * @param viewIdentifier The view identifier.
  * @param provider The name of the data source the should provide support for
  *                 persisted views.
  * @param options The view options.
  * @param allowExisting Determine what to do if a view with the same name already exists
  *                      in the data source catalog. If set to true nothing will happen
  *                      however if it is set to false then an exception will be thrown.
  */
case class CreatePersistentViewCommand(viewIdentifier: TableIdentifier,
                                       plan: LogicalPlan,
                                       provider: String,
                                       options: Map[String, String],
                                       allowExisting: Boolean)
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}

/**
  * Used to drop a persisted view in a given data source's catalog.
  *
  * @param viewIdentifier The view identifier.
  * @param provider The name of the data source the should provide support for
  *                 persisted views.
  * @param options The view options.
  * @param allowNotExisting Determine what to do if a view does not exist in the data source
  *                         catalog. If set to true nothing will happen however if it is set to
  *                         false then an exception will be thrown.
  */
private[sql]
case class DropPersistentViewCommand(viewIdentifier: TableIdentifier,
                                     provider: String,
                                     options: Map[String, String],
                                     allowNotExisting: Boolean)
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}
