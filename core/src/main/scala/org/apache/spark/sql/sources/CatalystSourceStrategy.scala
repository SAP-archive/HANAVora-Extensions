package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PartialAggregation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
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

  private def planNonPartitioned(cs: CatalystSource, plan: LogicalPlan): Seq[SparkPlan] =
    if (cs.supportsLogicalPlan(plan)) {
      execution.PhysicalRDD(plan.output, cs.logicalPlanToRDD(plan)) :: Nil
    } else {
      Nil
    }

  // scalastyle:off
  private def planPartitioned(cs: CatalystSource, plan: LogicalPlan): Seq[SparkPlan] = {

    @inline def isSupported(p: LogicalPlan): Boolean =
      !containsGlobalOperators(p) && cs.supportsLogicalPlan(p)

    @inline def toRDD(p: LogicalPlan): SparkPlan =
      execution.PhysicalRDD(p.output, cs.logicalPlanToRDD(p))

    plan match {
      case logical.Limit(IntegerLiteral(limit), logical.Distinct(logical.Sort(order, true, _)))
        if isSupported(plan) =>
        execution.Limit(limit,
          execution.Distinct(partial = false,
            execution.Sort(order, global = true,
              toRDD(plan)))) :: Nil
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, logical.Distinct(_)))
        if isSupported(plan) =>
        execution.Limit(limit,
          execution.Sort(order, global = true,
            execution.Distinct(partial = false,
              toRDD(plan)))) :: Nil
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, _))
        if isSupported(plan) =>
        execution.Limit(limit,
          execution.Sort(order, global = true,
            toRDD(plan))) :: Nil
      case logical.Limit(IntegerLiteral(limit), logical.Distinct(_))
        if isSupported(plan) =>
        execution.Limit(limit,
          execution.Distinct(partial = false,
            toRDD(plan))) :: Nil
      case logical.Limit(IntegerLiteral(limit), _)
        if isSupported(plan) =>
        execution.Limit(limit,
          toRDD(plan)) :: Nil
      case logical.Distinct(_)
        if isSupported(plan) =>
        execution.Distinct(partial = false,
          toRDD(plan)) :: Nil

      case partialAgg@PartialAggregation(
        finalGroupingAttributes,
        finalAggregateExpressions,
        partialGroupingExpressions,
        partialAggregateExpressions,
        child) =>

        val pushDownPlan = logical.Aggregate(
          partialGroupingExpressions,
          partialAggregateExpressions,
          child)
        if (isSupported(pushDownPlan)) {
          execution.Aggregate(
            partial = false,
            finalGroupingAttributes,
            finalAggregateExpressions,
            toRDD(pushDownPlan)
          ) :: Nil
        } else {
          Nil
        }

      case _: logical.Aggregate =>
        Nil
      case _
        if isSupported(plan) =>
        toRDD(plan) :: Nil
    }
  }

  private def containsGlobalOperators(plan: LogicalPlan): Boolean =
    plan
      .collect { case op => isGlobalOperation(op) }
      .forall { isGlobal => isGlobal }

  private def isGlobalOperation(op: LogicalPlan): Boolean =
    op match {
      case _: logical.Limit => true
      case _: logical.Sort => true
      case _: logical.Distinct => true
      case _: logical.Intersect => true
      case _: logical.Except => true
      case _: logical.Aggregate => true
      case _ => false
    }

}
