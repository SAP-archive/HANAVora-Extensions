package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, NodeType}

case class Hierarchy(
                      relation: LogicalPlan,
                      childAlias: String,
                      parenthoodExpression: Expression,
                      searchBy: Seq[SortOrder],
                      startWhere: Expression,
                      nodeAttribute: Attribute)
  extends UnaryNode {

  override def child: LogicalPlan = relation

  override def output: Seq[Attribute] = child.output :+ nodeAttribute

  /* TODO: This will be needed if we use generic Node inner types */
  private val joinDataType: Option[DataType] = {
    parenthoodExpression match {
      case be: BinaryExpression if be.resolved && be.left.dataType.sameType(be.right.dataType) =>
        Some(be.left.dataType)
      case x if !parenthoodExpression.resolved =>
        None
      case _ =>
        throw new IllegalArgumentException(
          "Hierarchy only supports binary expressions on JOIN PARENT"
        )
    }
  }

  private lazy val aliasedRelation: LogicalPlan =
      Subquery(childAlias, relation)

  override lazy val resolved: Boolean = !expressions.exists(!_.resolved) &&
    childrenResolved &&
    parenthoodExpression.resolved &&
    startWhere.resolved &&
    searchBy.map(_.resolved).forall(_ == true) &&
    nodeAttribute.resolved

  /**
   * From superclass:
   *
   * Attributes that are referenced by expressions but not provided by this nodes children.
   * Subclasses should override this method if they produce attributes internally as it is used by
   * assertions designed to prevent the construction of invalid plans.
   */
  override def missingInput: AttributeSet = references -- inputSet - nodeAttribute

  private def candidateAttributesForParenthoodExpression() : Seq[Attribute] =
    relation.output ++ aliasedRelation.output

  private[sql] def resolveParenthoodExpression(nameParts: Seq[String], resolver: Resolver)
  : Option[NamedExpression] =
    resolve(nameParts, candidateAttributesForParenthoodExpression(), resolver, true)

  private[sql] def resolveNodeAttribute() : Option[Attribute] =
    relation.resolved && parenthoodExpression.resolved match {
      case false if nodeAttribute.resolved => Some(nodeAttribute)
      case false => None
      case true if joinDataType.isDefined =>
        Some(AttributeReference(nodeAttribute.name, NodeType, nullable = false)())
      case _ => None
    }

}
