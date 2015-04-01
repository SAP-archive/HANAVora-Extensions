package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.test.HierarchyGen._
import org.apache.spark.sql.types.{Node, NodeType}
import org.scalatest.{MustMatchers, PropSpec}
import org.scalatest.prop.PropertyChecks

// scalastyle:off magic.number

class HierarchyExpressionsSuite extends PropSpec with PropertyChecks with MustMatchers {

  property("single node properties") {
    forAll(node, minSuccessful(100), maxDiscarded(1000), maxSize(20)) {
      (node : Node) => {
        val ref = BoundReference(0, NodeType, nullable = false)
        val row = Row(node)
        val level = Level(ref).eval(row)
        val isLeaf = IsLeaf(ref).eval(row)
        assert(level == node.path.size)
        assert(isLeaf == node.isLeaf.get)
      }
    }
  }

  property("pair properties for same node") {
    forAll(node, minSuccessful(100), maxDiscarded(1000), maxSize(20)) {
      (node: Node) => {
        val leftRef = BoundReference(0, NodeType, nullable = false)
        val rightRef = BoundReference(1, NodeType, nullable = false)
        val row = Row(node, node)
        val leftLevel = Level(leftRef).eval(row)
        val rightLevel = Level(rightRef).eval(row)
        val isDescendantLeftRight = IsDescendant(leftRef, rightRef).eval(row)
        val isDescendantRightLeft = IsDescendant(rightRef, leftRef).eval(row)
        val isDescendantOrSelfLeftRight = IsDescendantOrSelf(leftRef, rightRef).eval(row)
        val isDescendantOrSelfRightLeft = IsDescendantOrSelf(rightRef, leftRef).eval(row)
        val isParentLeftRight = IsParent(leftRef, rightRef).eval(row)
        val isParentRightLeft = IsParent(rightRef, leftRef).eval(row)
        val isSiblingLeftRight = IsSibling(leftRef, rightRef).eval(row)
        val isSiblingRightLeft = IsSibling(rightRef, leftRef).eval(row)
        assert(leftLevel == rightLevel)
        assert(isDescendantLeftRight == false)
        assert(isDescendantRightLeft == false)
        assert(isDescendantOrSelfLeftRight == true)
        assert(isDescendantOrSelfRightLeft == true)
        assert(isParentLeftRight == false)
        assert(isParentRightLeft == false)
        assert(isSiblingLeftRight == false)
        assert(isSiblingRightLeft == false)
      }
    }
  }

  property("pair properties for parent/child") {
    forAll(parentChildNodePair, minSuccessful(100), maxDiscarded(1000), maxSize(20)) {
      (nodePair: (Node,Node)) => {
        val parent = nodePair._1
        val child = nodePair._2
        val leftRef = BoundReference(0, NodeType, nullable = false)
        val rightRef = BoundReference(1, NodeType, nullable = false)
        val row = Row(parent, child)
        val leftLevel = Level(leftRef).eval(row)
        val rightLevel = Level(rightRef).eval(row)
        val isDescendantLeftRight = IsDescendant(leftRef, rightRef).eval(row)
        val isDescendantRightLeft = IsDescendant(rightRef, leftRef).eval(row)
        val isDescendantOrSelfLeftRight = IsDescendantOrSelf(leftRef, rightRef).eval(row)
        val isDescendantOrSelfRightLeft = IsDescendantOrSelf(rightRef, leftRef).eval(row)
        val isParentLeftRight = IsParent(leftRef, rightRef).eval(row)
        val isParentRightLeft = IsParent(rightRef, leftRef).eval(row)
        val isSiblingLeftRight = IsSibling(leftRef, rightRef).eval(row)
        val isSiblingRightLeft = IsSibling(rightRef, leftRef).eval(row)
        assert(leftLevel + 1 == rightLevel)
        assert(isDescendantLeftRight == false)
        assert(isDescendantRightLeft == true)
        assert(isDescendantOrSelfLeftRight == false)
        assert(isDescendantOrSelfRightLeft == true)
        assert(isParentLeftRight == true)
        assert(isParentRightLeft == false)
        assert(isSiblingLeftRight == false)
        assert(isSiblingRightLeft == false)
      }
    }
  }

  property("pair properties for siblings") {
    forAll(siblingNodePair, minSuccessful(100), maxDiscarded(1000), maxSize(20)) {
      (nodePair: (Node,Node)) => {
        val left = nodePair._1
        val right = nodePair._2
        val leftRef = BoundReference(0, NodeType, nullable = false)
        val rightRef = BoundReference(1, NodeType, nullable = false)
        val row = Row(left, right)
        val leftLevel = Level(leftRef).eval(row)
        val rightLevel = Level(rightRef).eval(row)
        val isDescendantLeftRight = IsDescendant(leftRef, rightRef).eval(row)
        val isDescendantRightLeft = IsDescendant(rightRef, leftRef).eval(row)
        val isDescendantOrSelfLeftRight = IsDescendantOrSelf(leftRef, rightRef).eval(row)
        val isDescendantOrSelfRightLeft = IsDescendantOrSelf(rightRef, leftRef).eval(row)
        val isParentLeftRight = IsParent(leftRef, rightRef).eval(row)
        val isParentRightLeft = IsParent(rightRef, leftRef).eval(row)
        val isSiblingLeftRight = IsSibling(leftRef, rightRef).eval(row)
        val isSiblingRightLeft = IsSibling(rightRef, leftRef).eval(row)
        assert(leftLevel == rightLevel)
        assert(isDescendantLeftRight == false)
        assert(isDescendantRightLeft == false)
        assert(isDescendantOrSelfLeftRight == false)
        assert(isDescendantOrSelfRightLeft == false)
        assert(isParentLeftRight == false)
        assert(isParentRightLeft == false)
        assert(isSiblingLeftRight == true)
        assert(isSiblingRightLeft == true)
      }
    }
  }

  property("pair properties") {
    forAll(unequalNodePair, minSuccessful(100), maxDiscarded(1000), maxSize(20)) {
      (nodePair : (Node, Node)) => {
        val leftRef = BoundReference(0, NodeType, nullable = false)
        val rightRef = BoundReference(1, NodeType, nullable = false)
        val row = Row(nodePair._1, nodePair._2)
        val leftLevel = Level(leftRef).eval(row)
        val rightLevel = Level(rightRef).eval(row)
        val isDescendantLeftRight = IsDescendant(leftRef, rightRef).eval(row)
        val isDescendantRightLeft = IsDescendant(rightRef, leftRef).eval(row)
        val isDescendantOrSelfLeftRight = IsDescendantOrSelf(leftRef, rightRef).eval(row)
        val isDescendantOrSelfRightLeft = IsDescendantOrSelf(rightRef, leftRef).eval(row)
        if (leftLevel <= rightLevel) {
          assert(isDescendantLeftRight == false)
          assert(isDescendantOrSelfLeftRight == false)
        }
        if (leftLevel >= rightLevel) {
          assert(isDescendantRightLeft == false)
          assert(isDescendantOrSelfRightLeft == false)
        }
        if (nodePair._1.path.startsWith(nodePair._2.path)) {
          assert(isDescendantOrSelfLeftRight == true)
        }
        if (nodePair._2.path.startsWith(nodePair._1.path)) {
          assert(isDescendantOrSelfRightLeft == true)
        }
      }
    }
  }

}
