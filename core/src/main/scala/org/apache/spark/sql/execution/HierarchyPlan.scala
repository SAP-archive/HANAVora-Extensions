package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{AdjacencyListHierarchySpec, HierarchySpec, LevelBasedHierarchySpec}
import org.apache.spark.sql.hierarchy._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.RddUtils

/**
  * The hierarchy execution.
  *
  * @param child The child plan.
  * @param spec The hierarchy spec.
  * @param node The node attribute of the output hierarchy table.
  */
private[sql] case class HierarchyPlan(child: SparkPlan, spec: HierarchySpec, node: Attribute)
  extends UnaryNode {

  private lazy val builder: HierarchyBuilder[Row, Row] = spec match {
    case AdjacencyListHierarchySpec(_, _, parenthoodExp, startWhere, orderBy) =>
      HierarchyRowBroadcastBuilder(child.output, parenthoodExp, startWhere, orderBy)
    case LevelBasedHierarchySpec(_, levels, startWhere, orderBy, matcher) =>
      HierarchyRowLevelBasedBuilder(
        child.output,
        levels,
        startWhere,
        orderBy,
        matcher)
  }

  override def output: Seq[Attribute] = child.output :+ node

  override def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()

    /** Copy to prevent weird duplicated rows. See SPARK-4775. */
    val mappedRDD = RddUtils.rowRddToRdd(rdd, child.schema)

    /** Build the hierarchy */
    val resultRdd = builder.buildHierarchyRdd(mappedRDD, spec.pathDataType)

    val cachedResultRdd = resultRdd.cache()

    /** Transform the result to Catalyst types */
    val schemaWithNode =
      StructType(child.schema.fields ++ Seq(StructField("", NodeType, nullable = false)))
    val resultInternalRdd = RDDConversions.rowToRowRdd(cachedResultRdd,
      schemaWithNode.fields.map(_.dataType))

    resultInternalRdd
  }
}
