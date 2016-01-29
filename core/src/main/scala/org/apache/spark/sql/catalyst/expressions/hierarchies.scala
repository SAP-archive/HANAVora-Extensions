package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

abstract class UnaryNodeExpression
  extends UnaryExpression
  with ImplicitCastInputTypes
  with CodegenFallback {
  self: Product =>

  override def nullable: Boolean = child.nullable
  protected def name: String
  override def toString(): String = s"$name($child)"
  override def inputTypes: Seq[AbstractDataType] = NodeType :: Nil

  override def nullSafeEval(input: Any): Any =
    nullSafeNodeEval(NodeType.deserialize(input))

  def nullSafeNodeEval(node: Node): Any
}

abstract class NodePredicate
  extends BinaryExpression
  with ImplicitCastInputTypes
  with CodegenFallback {
  self: Product =>

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[AbstractDataType] = Seq.fill(2)(NodeType)
  def symbol: String

  override def nullSafeEval(input1: Any, input2: Any): Any =
    nullSafeNodeEval(NodeType.deserialize(input1), NodeType.deserialize(input2))

  def nullSafeNodeEval(node1: Node, node2: Node): Any
}

case class Level(child: Expression) extends UnaryNodeExpression {
  override def dataType: DataType = IntegerType
  override protected def name = "LEVEL"
  override def nullable: Boolean = true
  override def nullSafeNodeEval(node: Node): Any =
    node.path.length
}

case class PreRank(child: Expression) extends UnaryNodeExpression {
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true  // need to be initialized
  override protected def name = "PRERANK"
  override def nullSafeNodeEval(node: Node): Any =
    node.preRank
}

case class PostRank(child: Expression) extends UnaryNodeExpression {
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true
  override protected def name = "POSTRANK"
  override def nullSafeNodeEval(node: Node): Any =
    node.postRank
}

case class IsRoot(child: Expression) extends UnaryNodeExpression {
  override def dataType: DataType = BooleanType
  override protected def name = "IS_ROOT"
  override def nullable: Boolean = true
  override def nullSafeNodeEval(node: Node): Any =
    node.path.length == 1
}

case class IsLeaf(child: Expression) extends UnaryNodeExpression {
  override def dataType: DataType = BooleanType
  override def nullable: Boolean = true
  override protected def name = "IS_LEAF"
  override def nullSafeNodeEval(node: Node): Any =
    node.isLeaf
}

case class IsDescendant(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_DESCENDANT"
  override def nullSafeNodeEval(leftNode: Node, rightNode: Node): Any =
    leftNode.path.size > rightNode.path.size && leftNode.path.startsWith(rightNode.path)
}

case class IsDescendantOrSelf(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_DESCENDANT_OR_SELF"
  override def nullSafeNodeEval(leftNode: Node, rightNode: Node): Any =
    leftNode.path.startsWith(rightNode.path)
}

case class IsParent(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_PARENT"
  override def nullSafeNodeEval(leftNode: Node, rightNode: Node): Any =
    leftNode.path == rightNode.path.slice(0, rightNode.path.size - 1)
}

case class IsSibling(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_SIBLING"
  override def nullSafeNodeEval(leftNode: Node, rightNode: Node): Any =
    leftNode.path.last != rightNode.path.last &&
      leftNode.path.slice(0, leftNode.path.size - 1) ==
        rightNode.path.slice(0, rightNode.path.size - 1)
}

case class IsSelf(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_SELF"
  override def nullSafeNodeEval(leftNode: Node, rightNode: Node): Any =
    leftNode == rightNode
}

case class IsSiblingOrSelf(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_SIBLING_OR_SELF"
  override def nullSafeNodeEval(leftNode: Node, rightNode: Node): Any =
    leftNode == rightNode || (leftNode.path.last != rightNode.path.last &&
      leftNode.path.slice(0, leftNode.path.size - 1) ==
        rightNode.path.slice(0, rightNode.path.size - 1))
}

case class IsFollowing(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_FOLLOWING"
  override def nullable: Boolean = false
  override def nullSafeNodeEval(leftNode: Node, rightNode: Node): Any = {
    val u: java.lang.Integer = leftNode.preRank
    val v: java.lang.Integer = rightNode.preRank
    val isDescendant =
      IsDescendant(left, right).nullSafeNodeEval(leftNode, rightNode)
      .asInstanceOf[Boolean]
    /** node u follows node v in pre-order and is not a descendant of v */
    u > v && !isDescendant
  }
}

case class IsPreceding(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_PRECEDING"
  override def nullable: Boolean = false
  override def nullSafeNodeEval(leftNode: Node, rightNode: Node): Any = {
    val u: java.lang.Integer = leftNode.preRank
    val v: java.lang.Integer = rightNode.preRank
    val isDescendant =
      IsDescendant(left, right).nullSafeNodeEval(leftNode, rightNode)
        .asInstanceOf[Boolean]
    /** node u precedes node v in pre-order and is not an ancestor of v */
    // IsDescendant(right,left) <=> IsAncestor(left, right):
    (u < v) && !isDescendant
  }
}
