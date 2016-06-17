package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan}

/**
  * Represents a self-join of two logical plans.
  *
  * @param left      The left logical plan of the self-join.
  * @param right     The right logical plan of the self-join.
  * @param joinType  The join type.
  * @param condition The join condition.
  * @param leftPath  The path from the left plan which is identical
  *                  in both sides of the join (is identical to
  *                  rightPath with normalized expressions).
  * @param rightPath The path from the right plan which is identical
  *                  in both sides of the join (is identical to
  *                  leftPath with normalized expressions).
  */
case class SelfJoin(left: LogicalPlan,
                    right: LogicalPlan,
                    joinType: JoinType,
                    condition: Option[Expression],
                    leftPath: LogicalPlan,
                    rightPath: LogicalPlan) extends BinaryNode {

  /** @inheritdoc */
  override def output: Seq[Attribute] = joinType match {
    case LeftSemi => left.output
    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    case RightOuter => left.output.map(_.withNullability(true)) ++ right.output
    case FullOuter =>
      left.output.map(_.withNullability(true)) ++
        right.output.map(_.withNullability(true))
    case _ =>
      left.output ++ right.output
  }

}
