package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

/**
 * Used to persist a given dimension view in a given data source's catalog.
 *
 * @param viewIdentifier The dimension view identifier.
 * @param provider The name of the data source the should provide support for
 *                 persisted dimension views.
 * @param options The dimension view options.
 * @param allowExisting Determine what to do if a view with the same name already exists
 *                      in the data source catalog. If set to true nothing will happen
 *                      however if it is set to false then an exception will be thrown.
 */
case class CreatePersistentDimensionViewCommand(viewIdentifier: TableIdentifier,
                                       plan: LogicalPlan,
                                       provider: String,
                                       options: Map[String, String],
                                       allowExisting: Boolean)
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}


