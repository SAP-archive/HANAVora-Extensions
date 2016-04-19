package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.IsLogicalRelation
import org.apache.spark.sql.sources.PartitionedRelation

/**
 * Re-orders [[LogicalPlan]]s so that co-located relations are in the same subtrees.
 * For example, the following logical plan tree:
 *
 *        O
 *      /  \
 *     O   T3(F)
 *   /  \
 *  T1   T2(F)
 *
 *  will be changed to the following one:
 *
 *        O
 *      /  \
 *    T1    O
 *        /  \
 *    T2(F) T3(F)
 *
 * by the left/right rotations.
 *
 * Legend:
 * T1 - unpartitioned table
 * T2, T3 - two tables partitioned by the same function F
 */
object AssureRelationsColocality extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    val subjectedToRotation = subjectToRotation(None, plan)
    if (subjectedToRotation.isDefined) {
      val (parent, eitherPlan) = subjectedToRotation.get
      /**
       * Perform the appropriate rotation (if the type of the join
       * and the structure of the plan allows to do it).
       */
      val rotatedPlan = eitherPlan match {
        case Left(pivot) => leftRotateLogicalPlan(pivot)
        case Right(pivot) => rightRotateLogicalPlan(pivot)
      }

      /**
       * Empty parent denotes that the join was in the root of the tree.
       * If the rotation took place in a descendant, we transform the plan
       * down in order to replace the descendant's subtree only.
       */
      if (parent.isEmpty) rotatedPlan else plan transformDown {
          case n if n == parent.get =>
            n.withNewChildren(Seq(rotatedPlan))
        }
    } else plan
  }

  /**
   * Checks whether any descendant in the logical plan can be subjected to right/left
   * rotation in order to improve co-locality for distributed joins. The first argument
   * is used only in recursive calls, so any external invocation of this method should
   * provide [[None]] for it.
   * Warning! This method does not verify whether the order of the children in the inner
   * join matters in the context of the join condition. This is deliberate, since it had
   * to be checked again in the right/left rotation methods, since their logic depends
   * on this characteristic of the subtree.
   *
   * @param parent The parent node of [[LogicalPlan]] given as the second argument,
   *               or [[None]] if the plan's root is being passed.
   * @param plan [[LogicalPlan]] which descendants are to be checked.
   * @return In case when no rotation might be executed in the provided plan in order to
   *         improve co-locality, [[None]] is returned.
   *         In case when a rotation may be executed (but not always can - see the warning above)
   *         the result constitutes a tuple (Option[LogicalPlan], Either[Join, Join]) where:
   *         - the first tuple element is the parent node of the join node, or [[None]]
   *           if the join node is the root of the logical plan,
   *         - the second tuple element is [[Either]] which defines whether the left rotation
   *           may be executed ([[Left]]) or the right rotation ([[Right]]) on the contained node.
   */
  // scalastyle:off cyclomatic.complexity
  private[this] def subjectToRotation(parent: Option[LogicalPlan], plan: LogicalPlan):
  Option[(Option[LogicalPlan], Either[Join, Join])] =
    plan match {
      // The join operator is the only one which can benefit from co-locality
      case p@Join(left, right, Inner, cond) if cond.isDefined =>
          // Collect PartitionedRelations on both sides of the join
          val leftRelations = getPartitionedRelations(left)
          val rightRelations = getPartitionedRelations(right)
          // Collect the attributes which are partitioned in the condition
          val partitioningAttrs = getPartitioningAttributes(left) ++
            getPartitioningAttributes(right)
          /**
           * If the upper join condition contains any other attributes, than the partitioned ones,
           * we would have to amend the join condition (TODO).
           */
          val upperConditionContainsOnlyPartitionedAttrs =
            filterConditionAttributes(partitioningAttrs, cond.get).isEmpty

          (left, right) match {
            /**
             * If the left sub-node is an inner join, and the right rotation prerequisites
             * are satisfied, return (parent, Right(current node)).
             */
            case (left: Join, _) if left.joinType == Inner
              && rightRotationConditionsSatisfied(leftRelations, rightRelations)
              && upperConditionContainsOnlyPartitionedAttrs => Some((parent, Right(p)))
            /**
             * If the right sub-node is an inner join, and the left rotation prerequisites
             * are satisfied, return (parent, Left(current node)).
             */
            case (_, right: Join) if right.joinType == Inner
              && leftRotationConditionsSatisfied(leftRelations, rightRelations)
              && upperConditionContainsOnlyPartitionedAttrs => Some((parent, Left(p)))
            // No rotation prerequisites are satisfied, return [[None]].
            case _ => None
          }
      /**
       * In case of an unary node we check the condition for the child,
       * passing the current node as the parent.
       */
      case p: UnaryNode => subjectToRotation(Some(p), p.child)
      // TODO: In case of the other binary operators we currently do nothing
      case _ => None
    }
  // scalastyle:on cyclomatic.complexity

  /**
   * Iterates a plan and for each [[PartitionedRelation]] attribute (that is defined in the
   * partitioning function) finds all of its referencing attributes and returns them (along with
   * the original relation attribute) in a sequence of [[ExprId]]. This step is necessary to solve
   * indirect referencing with attribute aliases.
   *
   * @param plan The logical plan to iterate.
   * @return A sequence of the partitioning function attributes along with their referencing
   *         attributes' ids (if any).
   */
  private[this] def getPartitioningAttributes(plan: LogicalPlan): Seq[ExprId] = {
    val keys = plan collect {
      case a@IsLogicalRelation(v: PartitionedRelation) =>
        val partitioningColumns = v.partitioningFunctionColumns
        // condition check is moved here instead of the case to reduce catalog calls.
        if (partitioningColumns.isDefined) {
          a.output.filter(attr => partitioningColumns.get.contains(attr.name))
        } else {
          Seq.empty[Attribute]
        }
    }

    keys.flatten.map { k =>
      var references = scala.collection.mutable.MutableList.empty[ExprId]
      plan transformAllExpressions {
        case e: NamedExpression if e.references.contains(k) =>
          references += e.exprId
          e
        case default => default
      }
      (k, references.toSet.toSeq)
    } flatMap {
      case (attr, refs) => refs :+ attr.exprId
    }
  }

  /**
   * Filters the given condition attributes by removing all attributes included
   * in the sequence provided as the first parameter.
   *
   * @param exprIds A sequence with the attributes to remove.
   * @param condition The condition which attributes are to be filtered.
   * @return A sequence with the filtered condition attributes.
   */
  private[this] def filterConditionAttributes(exprIds: Seq[ExprId],
                                              condition: Expression): Seq[ExprId] =
    condition match {
      case And(left, right) =>
        filterConditionAttributes(exprIds, left) ++
          filterConditionAttributes(exprIds, right)
      case EqualTo(left, right) =>
        filterConditionAttributes(exprIds, right) ++
          filterConditionAttributes(exprIds, left)
      case a: AttributeReference =>
        if (exprIds.contains(a.exprId)
          || a.references.map(_.exprId).toSet.subsetOf(exprIds.toSet)) {
          Seq.empty[ExprId]
        } else {
          a.references.map(_.exprId).toSeq :+ a.exprId
        }
      case _ =>
        condition.children.foldLeft(Seq.empty[ExprId]) {
          case (part, child) => part ++ filterConditionAttributes(exprIds, child)
        }
    }

  /**
   * Checks whether the relations on both sides of a join node (pivot) satisfy prerequisites
   * for the left rotation. The prerequisites constitute a conjunction of the following conditions:
   * - There are exactly two relations in the right subtree.
   * - There is exactly one relation in the left subtree, which is partitioned.
   * - Only one of the relations in the right subtree is partitioned, and its partitioning
   *   function is the same as for the relation in the left subtree.
   *
   * @param leftPivotRelations The relations on the left side of the pivot node.
   * @param rightPivotRotations The relations on the right side of the pivot node.
   * @return `true` if the mentioned conditions are satisfied, `false` otherwise.
   */
  private[this] def leftRotationConditionsSatisfied(leftPivotRelations: Seq[PartitionedRelation],
                                                    rightPivotRotations: Seq[PartitionedRelation]):
  Boolean = (leftPivotRelations.size == 1
    && rightPivotRotations.size == 2
    && leftPivotRelations.head.partitioningFunctionName.isDefined
    && isExactlyOneRelationPartitioned(rightPivotRotations)
    == leftPivotRelations.head.partitioningFunctionName)

  /**
   * Checks whether the relations on both sides of a join node (pivot) satisfy prerequisites
   * for the right rotation. The prerequisites constitute a conjunction of the following conditions:
   * - There are exactly two relations in the left subtree.
   * - There is exactly one relation in the right subtree, which is partitioned.
   * - Only one of the relations in the left subtree is partitioned, and its partitioning
   *   function is the same as for the relation in the right subtree.
   *
   * @param leftPivotRelations The relations on the left side of the pivot node.
   * @param rightPivotRotations The relations on the right side of the pivot node.
   * @return `true` if the mentioned conditions are satisfied, `false` otherwise.
   */
  private[this] def rightRotationConditionsSatisfied(leftPivotRelations: Seq[PartitionedRelation],
                                                     rightPivotRotations: Seq[PartitionedRelation]):
  Boolean = (leftPivotRelations.size == 2
    && rightPivotRotations.size == 1
    && rightPivotRotations.head.partitioningFunctionName.isDefined
    && isExactlyOneRelationPartitioned(leftPivotRelations)
    == rightPivotRotations.head.partitioningFunctionName)

  /**
   * Checks whether exactly one relation in the provided sequence is partitioned. As a result,
   * the method returns the name of the partitioning function of the relation, or [[None]]
   * if none of the relations are partitioned.
   * Warning! The method assumes that the provided sequence has exact;y two elements and
   * can return wrong results for sequences of other sizes.
   *
   * @param relations The sequence with the relations to check.
   * @return [[None]] if none of the relations in the provided sequence are partitioned.
   *         In case when exactly one relation is partitioned, its partitioning function's
   *         name is returned.
   */
  private[this] def isExactlyOneRelationPartitioned(relations: Seq[PartitionedRelation]):
  Option[String] =
    if (relations.head.partitioningFunctionName.isDefined
      && relations.last.partitioningFunctionName.isEmpty) {
      relations.head.partitioningFunctionName
    } else if (relations.head.partitioningFunctionName.isEmpty
      && relations.last.partitioningFunctionName.isDefined) {
      relations.last.partitioningFunctionName
    } else None

  /**
   * Performs left rotation of the provided logical plan (the provided argument is treated
   * as the rotation pivot). See AssureRelationsColocalitySuite for examples.
   *
   * @param plan The plan to rotate.
   * @return The (possibly) rotated plan.
   */
  private[this] def leftRotateLogicalPlan(plan: Join): LogicalPlan = {
    val pivot = plan.right.asInstanceOf[Join]
    if (getPartitionedRelations(pivot.left).head.partitioningFunctionName.isDefined) {
      val pivotLeft = plan.withNewChildren(Seq(plan.left, pivot.left))
      pivot.withNewChildren(Seq(pivotLeft, pivot.right))
    } else {
      val pivotLeft = plan.withNewChildren(Seq(plan.left, pivot.right))
      pivot.withNewChildren(Seq(pivotLeft, pivot.left))
    }
  }

  /**
   * Performs right rotation of the provided logical plan (the provided argument is treated
   * as the rotation pivot). See AssureRelationsColocalitySuite for examples.
   *
   * @param plan The plan to rotate.
   * @return The (possibly) rotated plan.
   */
  private[this] def rightRotateLogicalPlan(plan: Join): LogicalPlan = {
    val pivot = plan.left.asInstanceOf[Join]
    if (getPartitionedRelations(pivot.right).head.partitioningFunctionName.isDefined) {
      val pivotRight = plan.withNewChildren(Seq(pivot.right, plan.right))
      pivot.withNewChildren(Seq(pivot.left, pivotRight))
    } else {
      val pivotRight = plan.withNewChildren(Seq(pivot.left, plan.right))
      pivot.withNewChildren(Seq(pivot.right, pivotRight))
    }
  }

  /**
   * Returns all [[PartitionedRelation]]s from the provided plan.
   *
   * @param plan The plan, from which the relations are to be collected.
   * @return A sequence with the collected relations.
   */
  private[this] def getPartitionedRelations(plan: LogicalPlan): Seq[PartitionedRelation] =
    plan collect {
      case IsLogicalRelation(r: PartitionedRelation) => r
    }

}
