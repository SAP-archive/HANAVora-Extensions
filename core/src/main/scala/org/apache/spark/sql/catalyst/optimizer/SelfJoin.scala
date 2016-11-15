package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.util.PlanComparisonUtils
import org.apache.spark.sql.util.PlanUtils._

object SelfJoin {
  /**
    * The 'Self join' represented by the left and right original join join parts,
    * the join type, the condition and the left and right common sub-path.
    */
  type ReturnType =
    (LogicalPlan, // left
      LogicalPlan, // right
      JoinType, // join type
      Option[Expression], // join condition
      LogicalPlan, // join left path
      LogicalPlan) // join right path

  def unapply(join: Join): Option[ReturnType] = {
    if (isLinear(join.left) && isLinear(join.right)) {
      extractCommonPath(join.left, join.right).map {
        case (leftPath, rightPath) =>
          (join.left, join.right, join.joinType, join.condition, leftPath, rightPath)
      }
    } else None
  }

  /**
   * Extracts a common path in two [[LogicalPlan]]s starting from bottom. The plans
   * are assumed to contain only [[UnaryNode]]s or [[LeafNode]]s.
   *
   * @param p1 The first plan.
   * @param p2 The second plan.
   * @return [[None]] if the plans do not contain a common path,
   *         [[Some]] with the common paths, if there is a common subtree of the plans.
   */
  private[this] def extractCommonPath(p1: LogicalPlan, p2: LogicalPlan):
  Option[(LogicalPlan, LogicalPlan)] =
    p1.toPostOrderSeq.zip(p2.toPostOrderSeq).takeWhile {
      case(n1, n2) => comparePlanNodes(n1, n2)
    }.lastOption

  /**
   * Compares two nodes of [[LogicalPlan]]s after normalization of expressions.
   *
   * @param n1 The first node.
   * @param n2 The second node.
   * @return `true` if the nodes are the same after normalization of expressions IDs,
   *         `false` otherwise.
   */
  private[this] def comparePlanNodes(n1: LogicalPlan,
                                     n2: LogicalPlan): Boolean =
    (n1, n2) match {
      /**
       * For logical relation the output is generated automatically on-demand so we need to
       * check the underlying relations.
       */
      case (r1: LogicalRelation, r2: LogicalRelation) => r1.relation == r2.relation
      case (o1, o2) =>
        PlanComparisonUtils.normalizeExprIds(o1) == PlanComparisonUtils.normalizeExprIds(o2)
    }

  /**
   * Checks whether a [[LogicalPlan]] is a path.
   *
   * @param plan The plan to check.
   * @return `true` if the plan is a path, `false` otherwise.
   */
  private[this] def isLinear(plan: LogicalPlan): Boolean =
    plan.collect { case p => p }.forall(n => n.isInstanceOf[UnaryNode] || n.isInstanceOf[LeafNode])

}
