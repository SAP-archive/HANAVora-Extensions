package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LevelMatcher
import org.apache.spark.sql.hierarchy._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.RddUtils

/**
  * The hierarchy execution.
  *
  * @param child The child plan.
  * @param node The node attribute of the output hierarchy table.
  */
private[sql] abstract class HierarchyPlan(child: SparkPlan, node: Attribute)
  extends UnaryNode {

  protected val builder: HierarchyBuilder[Row, Row]

  protected val pathDataType: DataType

  override def output: Seq[Attribute] = child.output :+ node

  override def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()

    /** Copy to prevent weird duplicated rows. See SPARK-4775. */
    val mappedRDD = RddUtils.rowRddToRdd(rdd, child.schema)

    /** Build the hierarchy */
    val resultRdd = builder.buildHierarchyRdd(mappedRDD, pathDataType)

    val cachedResultRdd = resultRdd.cache()

    /** Transform the result to Catalyst types */
    val schemaWithNode =
      StructType(child.schema.fields ++ Seq(StructField("", NodeType, nullable = false)))
    val resultInternalRdd = RDDConversions.rowToRowRdd(cachedResultRdd,
      schemaWithNode.fields.map(_.dataType))

    resultInternalRdd
  }
}

private[sql] case class AdjacencyListHierarchyPlan(child: SparkPlan,
                                                   parenthoodExp: Expression,
                                                   startWhere: Option[Expression],
                                                   orderBy: Seq[SortOrder],
                                                   node: Attribute,
                                                   dataType: DataType)
  extends HierarchyPlan(child, node) {

  override protected val builder: HierarchyBuilder[Row, Row] =
      HierarchyRowBroadcastBuilder(child.output, parenthoodExp, startWhere, orderBy)

  override protected val pathDataType = dataType
}

private[sql] case class LevelHierarchyPlan(child: SparkPlan,
                                           levels: Seq[Expression],
                                           startWhere: Option[Expression],
                                           orderBy: Seq[SortOrder],
                                           matcher: LevelMatcher,
                                           node: Attribute,
                                           dataType: DataType)
  extends HierarchyPlan(child, node) {

  override protected val builder: HierarchyBuilder[Row, Row] =
    HierarchyRowLevelBasedBuilder(
      child.output,
      levels,
      startWhere,
      orderBy,
      matcher)

  override protected val pathDataType = dataType
}
