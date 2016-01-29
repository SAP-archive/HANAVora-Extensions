package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.{Attribute, BinaryExpression, Expression, SortOrder}
import org.apache.spark.sql.hierarchy._
import org.apache.spark.sql.catalyst.plans.logical.Hierarchy
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.RddUtils

/**
  * Execution for hierarchies.
  *
  * @param childAlias [[org.apache.spark.sql.catalyst.plans.logical.Hierarchy.childAlias]].
  * @param parenthoodExpression [[Hierarchy.parenthoodExpression]].
  * @param searchBy [[Hierarchy.searchBy]].
  * @param startWhere [[Hierarchy.startWhere]].
  * @param nodeAttribute [[Hierarchy.nodeAttribute]].
  * @param child [[Hierarchy.child]] as [[SparkPlan]].
  */
private[sql] case class HierarchyPhysicalPlan(
    childAlias: String,
    parenthoodExpression: Expression,
    searchBy: Seq[SortOrder],
    startWhere: Option[Expression],
    nodeAttribute: Attribute,
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output :+ nodeAttribute

  /**
    * Provides a [[HierarchyBuilder]]. Currently, we always use the
    * [[HierarchyRowBroadcastBuilder]].
    */
  private lazy val hierarchyBuilder: HierarchyBuilder[Row, Row] =
    HierarchyRowBroadcastBuilder(
      child.output,
      parenthoodExpression,
      startWhere,
      searchBy
    )

  override def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()

    val childSchema = child.schema

    /** Copy to prevent weird duplicated rows. See SPARK-4775. */
    val mappedRDD = RddUtils.rowRddToRdd(rdd, childSchema)

    /** Determine the type of the node elements */
    val pathDataType = parenthoodExpression.asInstanceOf[BinaryExpression].left.dataType

    /** Build the hierarchy */
    val resultRdd = hierarchyBuilder.buildFromAdjacencyList(mappedRDD, pathDataType)

    val cachedResultRdd = resultRdd.cache()

    /** Transform the result to Catalyst types */
    val schemaWithNode =
      StructType(childSchema.fields ++ Seq(StructField("", NodeType, nullable = false)))
    val resultInternalRdd = RDDConversions.rowToRowRdd(cachedResultRdd,
      schemaWithNode.fields.map(_.dataType))

    resultInternalRdd
  }

}
