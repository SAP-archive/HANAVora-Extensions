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
 * T1 - non-partitioned table
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
          // Alter the join condition if necessary and possible
          val condition = alterJoinConditionIfApplicable(left, right, cond.get)

          (left, right, condition) match {
            /**
             * If the left sub-node is an inner join, and the right rotation prerequisites
             * are satisfied, return (parent, Right(current node)).
             */
            case (left@Join(_, _, Inner, _), _, Some(c)) if
            rightRotationConditionsSatisfied(leftRelations, rightRelations) =>
              Some((parent, Right(Join(left, right, Inner, Some(c)))))
            /**
             * If the right sub-node is an inner join, and the left rotation prerequisites
             * are satisfied, return (parent, Left(current node)).
             */
            case (_, right@Join(_, _, Inner, _), Some(c)) if
            leftRotationConditionsSatisfied(leftRelations, rightRelations) =>
              Some((parent, Left(Join(left, right, Inner, Some(c)))))
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
