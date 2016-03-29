package org.apache.spark.sql.sources.describable

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._

/**
 * An object that is capable of describing itself.
 */
trait Describable {
  /**
   * Describes this in form of the specified [[DataType]].
   *
   * This method returns debug information.
   *
   * @return The result of the description.
   */
  def describe(): Any

  /**
    * The output [[DataType]] of the description.
 *
    * @return The output [[DataType]] of the description.
    */
  def describeOutput: DataType
}

object Describable {
  /**
    * Converts the given [[Any]] to the appropriate [[Describable]]
    *
    * If the given argument already is a [[Describable]] it will be return unchanged.
    * If it is a [[LogicalRelation]], it will return a [[LogicalRelationDescriber]].
    * Otherwise, if it is a [[LogicalPlan]], it will return a [[DefaultLogicalPlanDescriber]].
    * For all other cases, it will return a [[DefaultDescriber]].
    * @param any The [[Any]] to turn into a [[Describable]]
    * @return The corresponding [[Describable]]
    */
  def apply(any: Any): Describable = any match {
    case describable: Describable =>
      describable
    case logicalRelation: LogicalRelation =>
      LogicalRelationDescriber(logicalRelation)
    case logicalPlan: LogicalPlan =>
      DefaultLogicalPlanDescriber(logicalPlan)
    case default =>
      DefaultDescriber(default)
  }
}








