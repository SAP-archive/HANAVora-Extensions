package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.collection.immutable.Queue

/**
 * Utility functions to transform and iterate over [[TreeNode]]s.
 */
object PlanUtils {
  implicit class RichTreeNode[A <: TreeNode[A]](val plan: TreeNode[A]) {
    /**
     * Returns true if the plan has 0 children, false otherwise.
     * @return true if the plan has 0 children, false otherwise
     */
    def isLeaf: Boolean = plan.children.lengthCompare(0) == 0

    /**
     * Searches the plan pre-order and returns the first element that matches, [[None]] otherwise.
     * @param predicate A function that returns a boolean upon a [[TreeNode]]
     * @return [[Some]](matchedElement) if predicate was fulfilled, [[None]] otherwise
     */
    def find(predicate: A => Boolean): Option[A] = {
      plan.collectFirst {
        case p if predicate(p) => p
      }
    }

    /**
     * True if the predicate was successful for any of the elements in pre-order, false otherwise.
     * @param predicate A function that returns a boolean upon a [[TreeNode]]
     * @return True if the predicate was successful, false otherwise
     */
    def exists(predicate: A => Boolean): Boolean = {
      find(predicate).isDefined
    }

    /**
     * Checks if the given element is contained in the tree pre-order.
     * @param element The element to search for
     * @return True if the element is contained, false otherwise
     */
    def contains(element: A): Boolean = {
      exists(_ == element)
    }

    /**
     * Filters this tree pre-order for the elements that satisfy the predicate.
     * @param predicate A function that returns a boolean upon a [[TreeNode]]
     * @return A [[Seq]] consisting of elements that satisfied the predicate.
     */
    def filter(predicate: A => Boolean): Seq[A] = {
      plan.collect {
        case p if predicate(p) => p
      }
    }

    /**
     * Transforms the tree to a [[Seq]] with the specified [[TraversalType]]
     * @param traversalType The [[TraversalType]] to apply.
     * @return A [[Seq]] created by traversing the tree with the given [[TraversalType]]
     */
    def toSeq(traversalType: TraversalType): Seq[A] = traversalType match {
      case PreOrder => toPreOrderSeq
      case PostOrder => toPostOrderSeq
      case LevelOrder => toLevelOrderSeq
    }

    /**
     * Returns the tree by collecting its elements in pre-order
     * @return A [[Seq]] with the elements in pre-order
     */
    def toPreOrderSeq: Seq[A] = {
      (plan +: plan.children.view.flatMap(_.toPreOrderSeq)).asInstanceOf[Seq[A]]
    }

    /**
     * Returns the tree by collecting its elements in post-order
     * @return A [[Seq]] with the elements in post-order
     */
    def toPostOrderSeq: Seq[A] = {
      (plan.children.view.flatMap(_.toPostOrderSeq) :+ plan).asInstanceOf[Seq[A]]
    }

    /**
     * Returns the tree by collecting its elements in level-order
     * @return A [[Seq]] with the elements in level-order
     */
    def toLevelOrderSeq: Seq[A] = {
      def inner(acc: Queue[A], queue: Queue[A]): Seq[A] = {
        if (queue.isEmpty) {
          acc
        } else {
          val head = queue.head
          inner(acc :+ head, queue.tail ++ head.children)
        }
      }

      inner(Queue.empty, Queue(plan).asInstanceOf[Queue[A]])
    }
  }

  sealed trait TraversalType
  object PreOrder extends TraversalType
  object PostOrder extends TraversalType
  object LevelOrder extends TraversalType
}
