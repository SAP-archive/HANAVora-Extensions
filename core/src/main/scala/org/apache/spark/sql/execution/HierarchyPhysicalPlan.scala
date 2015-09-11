package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.hierarchy.HierarchyStrategy

case class HierarchyPhysicalPlan(childAlias: String,
                                 parenthoodExpression: Expression,
                                 searchBy: Seq[SortOrder],
                                 startWhere: Option[Expression],
                                 nodeAttribute: Attribute,
                                 child: SparkPlan) extends UnaryNode {

  override def doExecute(): RDD[Row] = HierarchyStrategy(
    child.output,
    parenthoodExpression,
    startWhere,
    searchBy
  ).execute(child.execute())

  override def output: Seq[Attribute] = child.output :+ nodeAttribute
}
