package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PartialAggregation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.compat._
import org.apache.spark.sql.execution.{CompatRDDConversions, SparkPlan}
import org.apache.spark.sql.sources.CatalystSource
import org.apache.spark.sql.{Strategy, execution}

private[sql] object CatalystSourceStrategy extends Strategy {

  // scalastyle:off cyclomatic.complexity
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val relations = plan.collect({ case p => p })
      .filter(_.isInstanceOf[LogicalRelation])
      .map(_.asInstanceOf[LogicalRelation])
      .map(_.relation)
      .toList

    if (relations.isEmpty || relations.exists(!_.isInstanceOf[CatalystSource])) {
      Nil
    } else {
      val sources = relations.map(_.asInstanceOf[CatalystSource])
      val source = sources.head
      val partitionedExecution = source.isMultiplePartitionExecution(sources)
      partitionedExecution match {
        case false => planNonPartitioned(source, plan)
        case true => planPartitioned(source, plan)
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def toPhysicalRDD(cs: CatalystSource, plan: LogicalPlan): SparkPlan = {
    val rdd = cs.logicalPlanToRDD(plan)
    val internalRdd = CompatRDDConversions.rddToRowRdd(rdd, plan.schema)
    physicalRDD(plan.output, internalRdd, "CatalystSource")
  }

  private def planNonPartitioned(cs: CatalystSource, plan: LogicalPlan): Seq[SparkPlan] =
    if (cs.supportsLogicalPlan(plan)) {
      toPhysicalRDD(cs, plan) :: Nil
    } else {
      Nil
    }

  // scalastyle:off
  private def planPartitioned(cs: CatalystSource, plan: LogicalPlan): Seq[SparkPlan] = {

    @inline def isSupported(p: LogicalPlan, nonGlobal: LogicalPlan): Boolean =
      !containsGlobalOperators(nonGlobal) && cs.supportsLogicalPlan(p)

    @inline def toRDD(p: LogicalPlan): SparkPlan =
      toPhysicalRDD(cs, p)

    plan match {
      case logical.Limit(IntegerLiteral(limit),
      logical.CompatDistinct(logical.Sort(order, globalSort, child)))
        if isSupported(plan, child) =>
        execution.Limit(limit,
          execution.CompatDistinct(
            execution.Sort(order, global = globalSort,
              toRDD(plan)))) :: Nil
      case logical.Limit(IntegerLiteral(limit),
      logical.Sort(order, globalSort, logical.CompatDistinct(child)))
        if isSupported(plan, child) =>
        execution.Limit(limit,
          execution.Sort(order, global = globalSort,
            execution.CompatDistinct(
              toRDD(plan)))) :: Nil
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, globalSort, child))
        if isSupported(plan, child) =>
        execution.Limit(limit,
          execution.Sort(order, global = globalSort,
            toRDD(plan))) :: Nil
      case logical.Limit(IntegerLiteral(limit), logical.CompatDistinct(child))
        if isSupported(plan, child) =>
        execution.Limit(limit,
          execution.CompatDistinct(
            toRDD(plan))) :: Nil
      case logical.Limit(IntegerLiteral(limit), child)
        if isSupported(plan, child) =>
        execution.Limit(limit,
          toRDD(plan)) :: Nil
      case logical.Distinct(child)
        if isSupported(plan, child) =>
        execution.CompatDistinct(
          toRDD(plan)) :: Nil

      case partialAgg@PartialAggregation(
        finalGroupings,
        finalAggregates,
        partialGroupings,
        partialAggregates,
        child) =>

        /* Avoid duplicate aliases */
        val fixedPartialAggregates = partialAggregates map {
          case a@Alias(c, name) =>
            Alias(c, name + a.exprId.id)(
              exprId = a.exprId,
              qualifiers = a.qualifiers,
              explicitMetadata = a.explicitMetadata
            )
          case other => other
        }

        val pushDownPlan = logical.Aggregate(
          partialGroupings,
          fixedPartialAggregates,
          child)
        if (isSupported(pushDownPlan, child)) {
          execution.Aggregate(
            partial = false,
            finalGroupings,
            finalAggregates,
            toRDD(pushDownPlan)
          ) :: Nil
        } else {
          Nil
        }

      case _: logical.Aggregate =>
        Nil
      case _
        if isSupported(plan, plan) =>
        toRDD(plan) :: Nil
      case _ =>
        Nil
    }
  }

  /**
    * Spark SQL optimizer converts [[logical.Distinct]] to a [[logical.Aggregate]]
    * grouping by all columns. This method detects such case.
    * @param agg
    * @return
    */
  private def isDistinct(agg: logical.Aggregate): Boolean = {
    agg.child.output == agg.groupingExpressions && agg.child.output == agg.aggregateExpressions
  }

  private def containsGlobalOperators(plan: LogicalPlan): Boolean =
    plan
      .collect { case op => isGlobalOperation(op) }
      .exists { isGlobal => isGlobal }

  private def isGlobalOperation(op: LogicalPlan): Boolean =
    op match {
      case _: logical.Limit => true
      case _: logical.Sort => true
      case _: logical.Distinct => true
      case _: logical.Intersect => true
      case _: logical.Except => true
      case _: logical.Aggregate => true
      case _: logical.Hierarchy => true
      case _ => false
    }

}
