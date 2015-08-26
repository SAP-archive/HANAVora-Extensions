package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types._

abstract class UnaryNodeExpression extends UnaryExpression {
  self: Product =>

  override def nullable: Boolean = child.nullable
  protected def name: String
  override def toString(): String = s"$name($child)"

  protected def check() {
    if (child.resolved && !child.dataType.sameType(NodeType)) {
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} does not support ${child.dataType} (only $dataType})"
      )
    }
  }
}

abstract class NodePredicate extends BinaryComparison {
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
  type EvaluatedType = java.lang.Integer
  override def dataType: DataType = IntegerType
  override protected def name = "LEVEL"
  override def nullable: Boolean = true
  override def eval(input: Row): EvaluatedType = {
    val node = child.eval(input).asInstanceOf[Node]
    if(node == null) {
      null
    } else {
      node.path.length
    }
  }

  check()
}

case class PreRank(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = java.lang.Integer
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true  // need to be initialized
  override protected def name = "PRERANK"

  override def eval(input: Row): EvaluatedType = {
    child.eval(input) match {
      case x: Node => x.preRank
      case _ => null
    }
  }

  check()
}

case class PostRank(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = java.lang.Integer
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true
  override protected def name = "POSTRANK"

  override def eval(input: Row): EvaluatedType = {
    child.eval(input) match {
      case x: Node => x.postRank
      case _ => null
    }
  }

  check()
}

case class IsRoot(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = java.lang.Boolean
  override def dataType: DataType = BooleanType
  override protected def name = "IS_ROOT"
  override def nullable: Boolean = true
  override def eval(input: Row): EvaluatedType = {
    val node = child.eval(input).asInstanceOf[Node]
    if(node == null) {
      null
    } else {
      node.path.length == 1
    }
  }

  check()
}

/**
 * This UDF is not part of the official list of UDFs we want to support.
 * TODO (YH, SM) either keep it and it to both Spark and HANA Vora
 * of remove it completely (#90029).
 */
case class IsLeaf(child: Expression) extends UnaryNodeExpression {
  type EvaluatedType = Any
  override def dataType: DataType = BooleanType
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
  override def nullable: Boolean = false

  override def eval(input: Row): Any = {
    val u: java.lang.Integer = PreRank(left).eval(input)
    val v: java.lang.Integer = PreRank(right).eval(input)
    /** node u follows node v in pre-order and is not a descendant of v */
    (u > v && !IsDescendant(left, right).eval(input).asInstanceOf[Boolean])
  }

  check()
}

case class IsPreceding(left: Expression, right: Expression) extends NodePredicate {
  override def symbol: String = "IS_PRECEDING"
  override def nullable: Boolean = false

  override def eval(input: Row): Any = {
    val u: java.lang.Integer = PreRank(left).eval(input)
    val v: java.lang.Integer = PreRank(right).eval(input)
    /** node u precedes node v in pre-order and is not an ancestor of v */
    // IsDescendant(right,left) <=> IsAncestor(left, right):
    (u < v) && !IsDescendant(right, left).eval(input).asInstanceOf[Boolean]
  }

  check()
}
