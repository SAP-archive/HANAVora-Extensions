package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions._

import FunctionBuilders._

private[sql] object RegisterHierarchyFunctions {

  def apply(functionRegistry: FunctionRegistry): Unit = {
    val r = (name: String, builder: ExpressionBuilder) =>
      functionRegistry.registerFunction(name, builder)
    r("level", unaryExpression[Level])
    r("post_rank", unaryExpression[PostRank])
    r("pre_rank", unaryExpression[PreRank])
    r("is_root", unaryExpression[IsRoot])
    r("is_descendant", binaryExpression[IsDescendant])
    r("is_descendant_or_self", binaryExpression[IsDescendantOrSelf])
    r("is_ancestor", reverse(binaryExpression[IsDescendant]))
    r("is_ancestor_or_self", reverse(binaryExpression[IsDescendantOrSelf]))
    r("is_parent", binaryExpression[IsParent])
    r("is_child", reverse(binaryExpression[IsParent]))
    r("is_sibling", binaryExpression[IsSibling])
    r("is_following", binaryExpression[IsFollowing])
    r("is_preceding", reverse(binaryExpression[IsFollowing]))
  }

}
