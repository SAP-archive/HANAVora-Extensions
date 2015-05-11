package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types._

private[expressions] abstract class UnaryNodeExpression extends UnaryExpression {
  self: Product =>

  override def nullable : Boolean = child.nullable
  protected def name : String
  override def toString() : String = s"$name($child)"

  protected def check() {
    if (child.resolved && !child.dataType.sameType(NodeType)) {
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} does not support ${child.dataType} (only $dataType})"
      )
    }
  }
}

private[expressions] abstract class NodePredicate extends BinaryComparison {
  self: Product =>

  protected def check() = {
    if (left.resolved && (!left.dataType.sameType(NodeType) ||
      !right.dataType.sameType(NodeType))) {
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} requires two arguments of type NodeType " +
          s"(got ${left.dataType} and ${right.dataType}"
      )
    }
  }
}

case class Level(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = Int
  override def dataType : DataType = IntegerType
  override protected def name = "LEVEL"

  override def eval(input: Row): EvaluatedType = {
    val node = child.eval(input).asInstanceOf[Node]
    node.path.length
  }

  check()
}

case class PreRank(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = java.lang.Integer
  override def dataType : DataType = IntegerType
  override def nullable: Boolean = true
  override protected def name = "PRERANK"

  override def eval(input: Row): EvaluatedType = {
    val node = child.eval(input).asInstanceOf[Node]
    node.preRank
  }

  check()
}

case class PostRank(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = java.lang.Integer
  override def dataType : DataType = IntegerType
  override def nullable: Boolean = true
  override protected def name = "POSTRANK"

  override def eval(input: Row): EvaluatedType = {
    val node = child.eval(input).asInstanceOf[Node]
    node.postRank
  }

  check()
}

case class IsRoot(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = java.lang.Boolean
  override def dataType : DataType = BooleanType
  override protected def name = "IS_ROOT"

  override def eval(input: Row): EvaluatedType = {
    val node = child.eval(input).asInstanceOf[Node]
    node.path.length == 1
  }

  check()
}

case class IsLeaf(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = Any
  override def dataType : DataType = BooleanType
  override def nullable: Boolean = true
  override protected def name = "IS_LEAF"

  override def eval(input: Row): EvaluatedType = {
    val node = child.eval(input).asInstanceOf[Node]
    node.isLeaf
  }

  check()
}

case class IsDescendant(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_DESCENDANT"

  override def eval(input: Row): EvaluatedType = {
    val leftNode = left.eval(input).asInstanceOf[Node]
    val rightNode = right.eval(input).asInstanceOf[Node]
    leftNode.path.size > rightNode.path.size && leftNode.path.startsWith(rightNode.path)
  }

  check()
}

case class IsDescendantOrSelf(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_DESCENDANT_OR_SELF"

  override def eval(input: Row): EvaluatedType = {
    val leftNode = left.eval(input).asInstanceOf[Node]
    val rightNode = right.eval(input).asInstanceOf[Node]
    leftNode.path.startsWith(rightNode.path)
  }

  check()
}

case class IsParent(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_PARENT"

  override def eval(input: Row): EvaluatedType = {
    val leftNode = left.eval(input).asInstanceOf[Node]
    val rightNode = right.eval(input).asInstanceOf[Node]
    leftNode.path == rightNode.path.slice(0, rightNode.path.size - 1)
  }

  check()
}

case class IsSibling(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_SIBLING"

  override def eval(input: Row): EvaluatedType = {
    val leftNode = left.eval(input).asInstanceOf[Node]
    val rightNode = right.eval(input).asInstanceOf[Node]
    leftNode.path.last != rightNode.path.last &&
      leftNode.path.slice(0, leftNode.path.size - 1) ==
        rightNode.path.slice(0, rightNode.path.size - 1)
  }

  check()
}

case class IsFollowing(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_FOLLOWING"
  override def nullable: Boolean = true

  override def eval(input: Row): Any = {
    val leftNode = left.eval(input).asInstanceOf[Node]
    val rightNode = right.eval(input).asInstanceOf[Node]
    if (leftNode.preRank == null || rightNode.preRank == null) {
      null
    } else {
      leftNode.preRank > rightNode.preRank
    }
  }

  check()
}
