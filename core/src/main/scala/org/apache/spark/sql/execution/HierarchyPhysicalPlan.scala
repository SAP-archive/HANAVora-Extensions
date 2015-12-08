package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.hierarchy.{HierarchyBuilder, HierarchyRowBroadcastBuilder}
import org.apache.spark.sql.types.{Node, NodeType, StructField, StructType}
import org.apache.spark.sql.catalyst.plans.logical.Hierarchy

/**
  * Execution for hierarchies.
  *
  * @param childAlias [[Hierarchy.childAlias]].
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

  override def doExecute(): RDD[Row] = {
    val rdd = child.execute()

    val childSchema = child.schema

    /** Copy to prevent weird duplicated rows. See SPARK-4775. */
    val mappedRDD = rdd.mapPartitions({ iter =>
      iter.map({ case r => Row.fromSeq(r.toSeq) })
    })

    /** Build the hierarchy */
    val resultRdd = hierarchyBuilder.buildFromAdjacencyList(mappedRDD)

    /** Transform the result to Catalyst types */
    val catalystResultRdd = resultRdd
      .mapPartitions({ iter =>
      val schemaWithNode =
        StructType(childSchema.fields ++ Seq(StructField("", NodeType, nullable = false)))
      val converter = CatalystTypeConverters.createToCatalystConverter(schemaWithNode)
      iter.map({ row =>
        val node = row.getAs[Node](row.length - 1)
        val rowWithoutNode = Row(row.toSeq.take(row.length - 1): _*)
        val convertedRowWithoutNode = converter(rowWithoutNode).asInstanceOf[Row]
        Row.fromSeq(convertedRowWithoutNode.toSeq :+ node)
      })
    })

    /** Return the cached result */
    catalystResultRdd.cache()
  }

}
