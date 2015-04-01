package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions._

private[sql] object RegisterHierarchyUDFs {

  def apply(functionRegistry: FunctionRegistry) : Unit = {
    functionRegistry.registerFunction("level", LevelFunction)
    functionRegistry.registerFunction("post_rank", PostRankFunction)
    functionRegistry.registerFunction("pre_rank", PreRankFunction)
    functionRegistry.registerFunction("is_root", IsRootFunction)
    functionRegistry.registerFunction("is_descendant", IsDescendantFunction)
    functionRegistry.registerFunction("is_descendant_or_self", IsDescendantOrSelfFunction)
    functionRegistry.registerFunction("is_ancestor", IsAncecestorFunction)
    functionRegistry.registerFunction("is_ancestor_or_self", IsAncecestorOrSelfFunction)
    functionRegistry.registerFunction("is_parent", IsParentFunction)
    functionRegistry.registerFunction("is_child", IsChildFunction)
    functionRegistry.registerFunction("is_sibling", IsSiblingFunction)
    functionRegistry.registerFunction("is_following", IsFollowingFunction)
    functionRegistry.registerFunction("is_preceding", IsPrecedingFunction)
  }

}

private[sql] object LevelFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 1) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 1)"
      )
    }
    Level(expressions.head)
  }
}

private[sql] object PreRankFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 1) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 1)"
      )
    }
    PreRank(expressions.head)
  }
}

private[sql] object PostRankFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 1) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 1)"
      )
    }
    PostRank(expressions.head)
  }
}

private[sql] object IsRootFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 1) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 1)"
      )
    }
    IsRoot(expressions.head)
  }
}

private[sql] object IsDescendantFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 2) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 2)"
      )
    }
    IsDescendant(expressions.head, expressions(1))
  }
}


private[sql] object IsAncecestorFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression =
    IsDescendantFunction(expressions.reverse)
}

private[sql] object IsDescendantOrSelfFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 2) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 2)"
      )
    }
    IsDescendantOrSelf(expressions.head, expressions(1))
  }
}


private[sql] object IsAncecestorOrSelfFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression =
    IsDescendantOrSelfFunction(expressions.reverse)
}

private[sql] object IsParentFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 2) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 2)"
      )
    }
    IsParent(expressions.head, expressions(1))
  }
}


private[sql] object IsChildFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression =
    IsParentFunction(expressions.reverse)
}

private[sql] object IsSiblingFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 2) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 2)"
      )
    }
    IsSibling(expressions.head, expressions(1))
  }
}


private[sql] object IsFollowingFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression = {
    if (expressions.size != 2) {
      throw new IllegalArgumentException(
        s"Invalid number of arguments: ${expressions.size} (must be equal to 2)"
      )
    }
    IsFollowing(expressions.head, expressions(1))
  }
}

private[sql] object IsPrecedingFunction extends Function[Seq[Expression],Expression] {
  override def apply(expressions: Seq[Expression]): Expression =
    IsFollowingFunction(expressions.reverse)
}
