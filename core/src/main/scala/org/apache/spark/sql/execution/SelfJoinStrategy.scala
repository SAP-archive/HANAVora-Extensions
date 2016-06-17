package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SelfJoin}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.extension.ExtendedPlanner
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode => LUnaryNode}

/**
  * Strategy to execute [[SelfJoin]]s.
  *
  * @param planner The [[ExtendedPlanner]].
  */
case class SelfJoinStrategy(planner: ExtendedPlanner) extends Strategy {

  /**
    * Resolves the [[SelfJoin]] node type correctly.
    *
    * @param plan The plan to check.
    * @return The plan with resolved [[SelfJoin]]s, if present.
    */
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case j@SelfJoin(left, right, joinType, cond, lp, rp) =>
      val child = planner.planLaterExt(lp)
      PhysicalSelfJoin(left, right, joinType, cond, child, j.output, lp, rp) :: Nil
    case _ => Nil
  }

}

/**
  * The physical implementation of the self-join resolution.
  *
  * @param left         The left side of the join.
  * @param right        The right side of the join.
  * @param joinType     The join type.
  * @param cond         The join condition.
  * @param child        The [[SparkPlan]] of the common path of the join's sides.
  * @param joinOutput   The join output specification.
  * @param leftSubtree  The left common subtree of the original join's logical plan.
  * @param rightSubtree The right common subtree of the original join's logical plan.
  */
case class PhysicalSelfJoin(left: LogicalPlan,
                            right: LogicalPlan,
                            joinType: JoinType,
                            cond: Option[Expression],
                            child: SparkPlan,
                            joinOutput: Seq[Attribute],
                            leftSubtree: LogicalPlan,
                            rightSubtree: LogicalPlan)
  extends SparkPlan {

  /** @inheritdoc */
  override protected def doExecute(): RDD[InternalRow] = {
    val leftRdd = new LogicalRDD(leftSubtree.output, child.execute())(sqlContext)
    val rightRdd = leftRdd.copy(output = rightSubtree.output)(sqlContext)
    val newLeft = replacePlanWithRDD(left, leftSubtree, leftRdd)
    val newRight = replacePlanWithRDD(right, rightSubtree, rightRdd)

    sqlContext.executePlan(Join(newLeft, newRight, joinType, cond)).toRdd
  }

  /** @inheritdoc */
  override def output: Seq[Attribute] = joinOutput

  /** @inheritdoc */
  override def children: Seq[SparkPlan] = Seq.empty

  /**
    * Replaces a part of a [[LogicalPlan]] with a [[LogicalRDD]].
    *
    * @param plan        The plan to replace.
    * @param toReplace   The node of the plan to replace.
    * @param replacement The [[LogicalRDD]] to substitute.
    * @return A new plan with the replacement.
    */
  private[this] def replacePlanWithRDD(plan: LogicalPlan,
                                       toReplace: LogicalPlan,
                                       replacement: LogicalRDD): LogicalPlan =
    if (plan == toReplace) replacement
    else plan.asInstanceOf[LUnaryNode].withNewChildren(Seq(
      replacePlanWithRDD(plan.asInstanceOf[LUnaryNode].child, toReplace, replacement)))

}
