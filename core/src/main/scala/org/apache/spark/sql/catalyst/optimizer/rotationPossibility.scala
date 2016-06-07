package org.apache.spark.sql.catalyst.optimizer

/**
 * This trait defines possible types of rotations of
 * [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]]s
 * which are used to denote in the optimizers which type of
 * the [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]]
 * tree rotation can be performed.
 */
sealed trait RotationPossibility

case object RotationImpossible extends RotationPossibility
case object RightRotationPossible extends RotationPossibility
case object LeftRotationPossible extends RotationPossibility
