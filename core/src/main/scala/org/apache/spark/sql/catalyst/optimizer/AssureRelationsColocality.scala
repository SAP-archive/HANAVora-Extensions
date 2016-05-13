package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.IsLogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, PartitionedRelation}

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
 * T1 - non-partitioned table
 * T2, T3 - two tables partitioned by the same function F
 */
object AssureRelationsColocality extends Rule[LogicalPlan] {

  /**
   * Checks whether any descendant in the logical plan can be subjected to right/left
   * rotation in order to improve co-locality for distributed joins and rotates them
   * if possible.
   *
   * @param plan A [[LogicalPlan]] which descendants are to be rotated.
   * @return The given plan with some rotations of joins possibly executed.
   */
  def apply(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      // The join operator is the only one which can benefit from co-locality
      case p@Join(left, right, Inner, Some(cond)) =>
        // Collect logical relations on both sides of the join
        val leftRelations = getLogicalRelations(left)
        val rightRelations = getLogicalRelations(right)

        rotateJoinsIfConditionMatches(left, right, cond,
          rotationConditionsSatisfied(leftRelations, rightRelations))
      case p: BinaryNode =>
        p.withNewChildren(Seq(apply(p.left), apply(p.right)))
      /**
       * In case of an unary node we check the condition for the child,
       * passing the current node as the parent.
       */
      case p: UnaryNode => p.withNewChildren(Seq(apply(p.child)))
      // This catches leaf nodes
      case p: LeafNode => p.withNewChildren(p.children.map(c => apply(c)))
      // Catch the other types of nodes
      case p => p
    }

  /**
   * Performs rotation of [[Join]] nodes in a [[LogicalPlan]] if the rotation conditions
   * provided in the last argument enable some rotations to be performed.
   *
   * @param left The left child of the [[Join]] node to rotate.
   * @param right The right child of the [[Join]] node to rotate.
   * @param condition The join condition.
   * @param rotationPossibility The rotation possibility. [[LeftRotationPossible]]
   *                            or [[RightRotationPossible]] denote that the left
   *                            or right rotation can be executed respectively.
   *                            [[RotationImpossible]] denotes that no rotation
   *                            can be executed.
   * @return The possibly rotated [[LogicalPlan]].
   */
  // scalastyle:off method.length
  private[this] def rotateJoinsIfConditionMatches(left: LogicalPlan,
                                                  right: LogicalPlan,
                                                  condition: Expression,
                                                  rotationPossibility: RotationPossibility):
  LogicalPlan = {
    // Alter the join condition if necessary and possible
    val alteredCondition = alterJoinConditionIfApplicable(left, right, condition)

    (left, right, alteredCondition, rotationPossibility) match {
      /**
       * If the left sub-node is an inner join, and the right rotation prerequisites
       * are satisfied, return (parent, Right(current node)).
       */
      case (left@Join(_, _, Inner, _), _, Some(c), RightRotationPossible) =>
        rightRotateLogicalPlan(Join(left, right, Inner, Some(c)), withProjections = false)

      /**
       * The same as above, but with attributes' projection handling. For example, this tree:
       *
       *                        Join(id2 = id3)
       *                        /            \
       *                       /              \
       *                      /                \
       *               Project(id1, id2)      Project(id3)
       *                    |                    |
       *               Join(id1 = id2)         T3 (F)(id3)
       *               /           \
       *         Project(id1)    Project(id2)
       *              /             \
       *           T1 (id1)     T2 (F)(id2)
       *
       * should be changed to:
       *
       *                        Join(id1 = id2)
       *                        /            \
       *                       /              \
       *                      /                \
       *               Project(id1)       Project(id2, id3)
       *                    |                    |
       *               T1 (id1)           Join(id2 = id3)
       *                                   /          \
       *                             Project(id2)   Project(id3)
       *                                  |            |
       *                             T2 (F)(id2)   T3 (F)(id3)
       */
      case (left@Project(projections, child@Join(_, _, Inner, _)),
      _, Some(c), RightRotationPossible) =>
        val partitioningAttrs = getPartitioningAttributes(child)
        val projectionsToMove = projections.filter(p => partitioningAttrs.contains(p.exprId))
        val updatedRight = right match {
          case r@Project(rProjections, other) =>
            Project(projectionsToMove ++ rProjections, r)
          case other =>
            Project(projectionsToMove, other)
        }
        rightRotateLogicalPlan(Join(child, updatedRight, Inner, Some(c)),
          withProjections = true)

      /**
       * If the right sub-node is an inner join, and the left rotation prerequisites
       * are satisfied, return (parent, Left(current node)).
       */
      case (_, right@Join(_, _, Inner, _), Some(c), LeftRotationPossible) =>
        leftRotateLogicalPlan(Join(left, right, Inner, Some(c)),
          withProjections = false)

      /**
       * The same as above, but with attributes' projection handling.
       */
      case (_, right@Project(projections, child@Join(_, _, Inner, _)),
      Some(c), LeftRotationPossible) =>
        val partitioningAttrs = getPartitioningAttributes(child)
        val projectionsToMove = projections.filter(p => partitioningAttrs.contains(p.exprId))
        val updatedLeft = left match {
          case l@Project(lProjections, other) =>
            Project(lProjections ++ projectionsToMove, l)
          case other =>
            Project(projectionsToMove, other)
        }
        leftRotateLogicalPlan(Join(updatedLeft, child, Inner, Some(c)),
          withProjections = true)
      // No rotation prerequisites are satisfied, process the subtrees
      case _ => Join(apply(left), apply(right), Inner, Some(condition))
    }
  }
  // scalastyle:on method.length

  /**
   * Alters a join condition, if applicable. The join condition is required
   * to be altered if the upper join condition refers to a column from the
   * non-partitioned table, e.g.:
   *
   *          (id1=id3)
   *            /  \
   *           /    \
   *          /      \
   *     (id1=id2)   T3(F)(id3)
   *       /  \
   * T1(id1)   T2(F)(id2)
   *
   * id1, id2 and id3 are columns of the tables T1, T2 and T3 respectively.
   *
   * However, the upper join condition (id1=id3) in the tree above can be altered
   * to (id2=id3) only if there are no references to id1 in the whole plan.
   *
   * @param left The left subtree of the join to alter.
   * @param right The right subtree of the join to alter.
   * @param condition The join condition.
   * @return [[None]] if the join condition was required to have been altered but
   *         its alteration attempt did not succeed. [[Some]] if the condition
   *         remained unchanged (the upper condition does not contain columns
   *         from the non-partitioned table) or was successfully altered.
   */
  private[this] def alterJoinConditionIfApplicable(left: LogicalPlan,
                                                   right: LogicalPlan,
                                                   condition: Expression):
  Option[Expression] = {
    // Collect the attributes which are partitioned in the join
    val partitioningAttrs = getPartitioningAttributes(left) ++
      getPartitioningAttributes(right)
    // Get the non-partitioned attributes from the join condition
    val nonPartitionedConditionAttrs = filterConditionAttributes(partitioningAttrs, condition)

    nonPartitionedConditionAttrs.toSeq match {
      case Seq(head: Attribute) =>
        /**
         * If the upper join condition contains any other attributes than the partitioned ones,
         * we need to replace them with the partitioned attributes, but only if they are not
         * referenced above in the plan (the references' set contains at most the attribute itself).
         */
        if (head.references.size <= 1) {
          val attrToReplace = nonPartitionedConditionAttrs.head
          val attrsMap = (left, right) match {
            case (l@Join(_, _, _, Some(lCond)), _) =>
              getConditionAttributeMapping(l.condition.get)
            case (_, r@Join(_, _, _, Some(rCond))) =>
              getConditionAttributeMapping(r.condition.get)
            case _ => Map.empty[Attribute, Attribute]
          }

          if (attrsMap.contains(attrToReplace)) {
            Some(replaceConditionAttribute(condition, attrToReplace, attrsMap(attrToReplace)))
          }
          // Attribute replacement did not succeed, return [[None]]
          else None
        }
        // Attribute replacement did not succeed, return [[None]]
        else None
      /**
       * Either we have 0 attributes, so then we return a condition without any changes (it does
       * not require altering). The cases with 2 or more attributes are currently not supported.
       */
      case _ => Some(condition)
    }
  }

  /**
   * Returns a mapping between all attributes in a condition which are expected to be equal.
   *
   * @param condition The condition from which the mapping is to be extracted.
   * @return The mapping containing the attributes which are equal in the condition.
   *         The mapping is bi-directional, i.e. if a condition says that a = b,
   *         the mapping will contain two entries: a -> b and b -> a.
   */
  private[this] def getConditionAttributeMapping(condition: Expression):
  Map[Attribute, Attribute] = condition match {
    case And(left, right) =>
      getConditionAttributeMapping(left) ++
        getConditionAttributeMapping(right)
    case EqualTo(left: Attribute, right: Attribute) =>
      Map(left -> right, right -> left)
    case _ => Map.empty[Attribute, Attribute]
  }

  /**
   * Replaces an attribute in a condition expression.
   *
   * @param condition The condition in which the attributes are supposed to be replaced.
   * @param toReplace The attribute to replace.
   * @param replacement The replacement attribute.
   * @return The altered condition (with the attribute replaced).
   */
  private[this] def replaceConditionAttribute(condition: Expression,
                                              toReplace: Attribute,
                                              replacement: Attribute): Expression =
    condition match {
      case And(left, right) =>
        And(replaceConditionAttribute(left, toReplace, replacement),
          replaceConditionAttribute(right, toReplace, replacement))
      case EqualTo(left, right) =>
        EqualTo(replaceConditionAttribute(left, toReplace, replacement),
          replaceConditionAttribute(right, toReplace, replacement))
      case a: Attribute if a.equals(toReplace) => replacement
      case _ =>
        condition.withNewChildren(condition.children.map(c =>
          replaceConditionAttribute(c, toReplace, replacement)))
    }

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
   * @return A set with the filtered condition attributes.
   */
  private[this] def filterConditionAttributes(exprIds: Seq[ExprId],
                                              condition: Expression): Set[Attribute] =
    (condition match {
      case And(left, right) =>
        filterConditionAttributes(exprIds, left) ++
          filterConditionAttributes(exprIds, right)
      case EqualTo(left, right) =>
        filterConditionAttributes(exprIds, left) ++
          filterConditionAttributes(exprIds, right)
      case a: AttributeReference =>
        if (exprIds.contains(a.exprId)
          || a.references.map(_.exprId).toSet.subsetOf(exprIds.toSet)) {
          Seq.empty[Attribute]
        } else {
          a.references.toSeq :+ a
        }
      case _ =>
        condition.children.foldLeft(Seq.empty[Attribute]) {
          case (part, child) => part ++ filterConditionAttributes(exprIds, child)
        }
    }).toSet

  /**
   * Checks whether the relations on both sides of a join node (pivot) satisfy prerequisites
   * for the left or the right rotation. The prerequisites constitute a conjunction of
   * the following conditions:
   * - For the left rotation:
   *   * There are exactly two relations in the right subtree.
   *   * There is exactly one relation in the left subtree, which is partitioned.
   *   * Only one of the relations in the right subtree is partitioned, and its partitioning
   *     function is the same as for the relation in the left subtree.
   * - For the right rotation:
   *   * There are exactly two relations in the left subtree.
   *   * There is exactly one relation in the right subtree, which is partitioned.
   *   * Only one of the relations in the left subtree is partitioned, and its partitioning
   *     function is the same as for the relation in the right subtree.
   * The function returns one of the [[RotationPossibility]] subtypes.
   *
   * @param leftPivotRelations The relations on the left side of the pivot node.
   * @param rightPivotRotations The relations on the right side of the pivot node.
   * @return [[LeftRotationPossible]] if it is possible to execute the left rotation.
   *         [[RightRotationPossible]] if it is possible to execute the right rotation.
   *         [[RotationImpossible]] if no rotation can be executed.
   */
  private[this] def rotationConditionsSatisfied(leftPivotRelations: Seq[BaseRelation],
                                                rightPivotRotations: Seq[BaseRelation]):
  RotationPossibility = (leftPivotRelations, rightPivotRotations) match {
    case (Seq(p: PartitionedRelation), rights) if rights.size == 2 &&
      p.partitioningFunctionName.isDefined &&
      isExactlyOneRelationPartitioned(rights) == p.partitioningFunctionName =>
      LeftRotationPossible
    case (lefts, Seq(p: PartitionedRelation)) if lefts.size == 2 &&
      p.partitioningFunctionName.isDefined &&
      isExactlyOneRelationPartitioned(leftPivotRelations) == p.partitioningFunctionName =>
      RightRotationPossible
    case _ => RotationImpossible
  }

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
  private[this] def isExactlyOneRelationPartitioned(relations: Seq[BaseRelation]):
  Option[String] = (relations.head, relations.last) match {
    case (h: PartitionedRelation, l) if h.partitioningFunctionName.isDefined
      && (!l.isInstanceOf[PartitionedRelation]
      || l.asInstanceOf[PartitionedRelation].partitioningFunctionName.isEmpty) =>
      h.partitioningFunctionName
    case (h, l: PartitionedRelation) if l.partitioningFunctionName.isDefined
      && (!h.isInstanceOf[PartitionedRelation]
      || h.asInstanceOf[PartitionedRelation].partitioningFunctionName.isEmpty) =>
      l.partitioningFunctionName
    case _ => None
  }

  /**
   * Performs left rotation of the provided logical plan (the provided argument is treated
   * as the rotation pivot). See AssureRelationsColocalitySuite for examples.
   *
   * @param plan The plan to rotate.
   * @param withProjections Should be set to `true` if the left child of the plan
   *                        contains a [[Project]] node which should be placed above
   *                        the resulting lower [[Join]] node.
   * @return The (possibly) rotated plan.
   */
  private[this] def leftRotateLogicalPlan(plan: Join, withProjections: Boolean): LogicalPlan = {
    val Join(_, pivot: Join, _, _) = plan
    getLogicalRelations(pivot.left).head match {
      case r: PartitionedRelation if r.partitioningFunctionName.isDefined =>
        val pivotLeft = if (withProjections) {
          plan.left.withNewChildren(Seq(
            plan.withNewChildren(plan.left.children ++ Seq(pivot.left))))
        } else plan.withNewChildren(Seq(plan.left, pivot.left))
        pivot.withNewChildren(Seq(pivotLeft, pivot.right))
      case _ =>
        val pivotLeft = plan.withNewChildren(Seq(plan.left, pivot.right))
        pivot.withNewChildren(Seq(pivotLeft, pivot.left))
    }
  }

  /**
   * Performs right rotation of the provided logical plan (the provided argument is treated
   * as the rotation pivot). See AssureRelationsColocalitySuite for examples.
   *
   * @param plan The plan to rotate.
   * @param withProjections Should be set to `true` if the right child of the plan
   *                        contains a [[Project]] node which should be placed above
   *                        the resulting lower [[Join]] node.
   * @return The (possibly) rotated plan.
   */
  private[this] def rightRotateLogicalPlan(plan: Join, withProjections: Boolean): LogicalPlan = {
    val Join(pivot: Join, _, _, _) = plan
    getLogicalRelations(pivot.right).head match {
      case r: PartitionedRelation if r.partitioningFunctionName.isDefined =>
        val pivotRight = if (withProjections) {
          plan.right.withNewChildren(Seq(
            plan.withNewChildren(Seq(pivot.right) ++ plan.right.children)))
        } else plan.withNewChildren(Seq(pivot.right, plan.right))
        pivot.withNewChildren(Seq(pivot.left, pivotRight))
      case _ =>
        val pivotRight = plan.withNewChildren(Seq(pivot.left, plan.right))
        pivot.withNewChildren(Seq(pivot.right, pivotRight))
    }
  }

  /**
   * Returns all logical relations from the provided plan.
   *
   * @param plan The plan, from which the relations are to be collected.
   * @return A sequence with the collected relations.
   */
  private[this] def getLogicalRelations(plan: LogicalPlan): Seq[BaseRelation] =
    plan collect {
      case IsLogicalRelation(r: BaseRelation) => r
    }

}
