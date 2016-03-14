package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.immutable.Queue

/**
 * Utility functions to transform and iterate over [[LogicalPlan]]s.
 */
object PlanUtils {
  implicit class RichTreeNode(val plan: LogicalPlan) {
    /**
     * Returns true if the plan has 0 children, false otherwise.
     * @return true if the plan has 0 children, false otherwise
     */
    def isLeaf: Boolean = plan.children.lengthCompare(0) == 0

    /**
     * Searches the plan pre-order and returns the first element that matches, [[None]] otherwise.
     * @param predicate A function that returns a boolean upon a [[LogicalPlan]]
     * @return [[Some]](matchedElement) if predicate was fulfilled, [[None]] otherwise
     */
    def find(predicate: LogicalPlan => Boolean): Option[LogicalPlan] = {
      plan.collectFirst {
        case p if predicate(p) => p
      }
    }

    /**
     * True if the predicate was successful for any of the elements in pre-order, false otherwise.
     * @param predicate A function that returns a boolean upon a [[LogicalPlan]]
     * @return True if the predicate was successful, false otherwise
     */
    def exists(predicate: LogicalPlan => Boolean): Boolean = {
      find(predicate).isDefined
    }

    /**
     * Checks if the given element is contained in the tree pre-order.
     * @param element The element to search for
     * @return True if the element is contained, false otherwise
     */
    def contains(element: LogicalPlan): Boolean = {
      exists(_ == element)
    }

    /**
     * Filters this tree pre-order for the elements that satisfy the predicate.
     * @param predicate A function that returns a boolean upon a [[LogicalPlan]]
     * @return A [[Seq]] consisting of elements that satisfied the predicate.
     */
    def filter(predicate: LogicalPlan => Boolean): Seq[LogicalPlan] = {
      plan.collect {
        case p if predicate(p) => p
      }
    }

    /**
     * Transforms the tree to a [[Seq]] with the specified [[TraversalType]]
     * @param traversalType The [[TraversalType]] to apply.
     * @return A [[Seq]] created by traversing the tree with the given [[TraversalType]]
     */
    def toSeq(traversalType: TraversalType): Seq[LogicalPlan] = traversalType match {
      case PreOrder => toPreOrderSeq
      case PostOrder => toPostOrderSeq
      case LevelOrder => toLevelOrderSeq
    }

    /**
     * Returns the tree by collecting its elements in pre-order
     * @return A [[Seq]] with the elements in pre-order
     */
    def toPreOrderSeq: Seq[LogicalPlan] = {
      plan +: plan.children.view.flatMap(_.toPreOrderSeq)
    }

    /**
     * Returns the tree by collecting its elements in post-order
     * @return A [[Seq]] with the elements in post-order
     */
    def toPostOrderSeq: Seq[LogicalPlan] = {
      plan.children.view.flatMap(_.toPostOrderSeq) :+ plan
    }

    /**
     * Returns the tree by collecting its elements in level-order
     * @return A [[Seq]] with the elements in level-order
     */
    def toLevelOrderSeq: Seq[LogicalPlan] = {
      def inner(acc: Queue[LogicalPlan], queue: Queue[LogicalPlan]): Seq[LogicalPlan] = {
        if (queue.isEmpty) {
          acc
        } else {
          val head = queue.head
          inner(acc :+ head, queue.tail ++ head.children)
        }
      }

      inner(Queue.empty, Queue(plan))
    }
  }

  sealed trait TraversalType
  object PreOrder extends TraversalType
  object PostOrder extends TraversalType
  object LevelOrder extends TraversalType
}
