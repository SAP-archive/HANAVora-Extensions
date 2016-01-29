package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PartialAggregation}
import org.apache.spark.sql.execution.aggregate.{SortBasedAggregate, TungstenAggregate}
import org.apache.spark.sql.execution.{PhysicalRDD, RDDConversions, SparkPlan}
import org.apache.spark.sql.sources.CatalystSource
import org.apache.spark.sql.{Strategy, execution}
import org.apache.spark.sql.catalyst.expressions.aggregate._

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
    val internalRdd = RDDConversions.rowToRowRdd(rdd,
      plan.schema.fields.map(_.dataType))

    PhysicalRDD(plan.output, internalRdd, "CatalystSource")
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

    /**
      * Before the Spark 1.6 migration the CatalystSource Strategy also distinguished in
      * cases the query asked for DISTINCT elements. However, this has been buried into aggregate
      * expressions - thus it is handeld in the partial aggregation
      */
    plan match {
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, globalSort, child))
        if isSupported(plan, child) =>
        execution.Limit(limit,
          execution.Sort(order, global = globalSort, child = toRDD(plan))) :: Nil
      case logical.Limit(IntegerLiteral(limit), child)
        if isSupported(plan, child) =>
        execution.Limit(limit,
            toRDD(plan)) :: Nil

      case partialAgg@PartialAggregation(
        finalGroupings,
        finalAggregates,
        partialGroupings,
        partialAggregates,
        aggregateFunctionToAttributeMap,
        resultExpressions,
        child) =>

         // compose plan that has to be done by the datasource
         val pushDownPlan =
           logical.Aggregate(partialGroupings, partialAggregates, child)

        if (isSupported(pushDownPlan, child)) {
          planAggregateWithoutDistinctWithPushdown(
            groupingExpressions = finalGroupings,
            aggregateExpressions = finalAggregates,
            aggregateFunctionToAttribute = aggregateFunctionToAttributeMap,
            resultExpressions = resultExpressions,
            pushedDownChild = toRDD(pushDownPlan))
        } else {
          Nil
        }

      // partitioned and aggregates do not go together, should have matched in the
      // partial aggregation part before
      case _: logical.Aggregate =>
        Nil
      case _ if isSupported(plan, plan) =>
        toRDD(plan) :: Nil
      case _ =>
        Nil
    }
  }

  /**
    * Spark SQL optimizer converts [[logical.Distinct]] to a [[logical.Aggregate]]
    * grouping by all columns. This method detects such case.
    *
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

  /**
    * This emits a Spark Plan for partial aggregations taking Tungsten (if possible) into account.
    *
    * In contrast to the already present planning methods in
    * [[org.apache.spark.sql.execution.aggregate.Utils]] this assumes that the partial aggregation
    * happens in the "child", i.e., the pushed down data source.
    *
    * @param groupingExpressions grouping expressions of the final aggregate
    * @param aggregateExpressions aggregate expressions of the final aggregate
    * @param aggregateFunctionToAttribute mapping from attributes to aggregation functions
    * @param resultExpressions result expressions of this plan
    * @param pushedDownChild the child returning the partial aggregation
    * @return Spark plan including pushed down parts
    */
  private def planAggregateWithoutDistinctWithPushdown(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateFunctionToAttribute: Map[(AggregateFunction, Boolean), Attribute],
    resultExpressions: Seq[NamedExpression],
    pushedDownChild: SparkPlan): Seq[SparkPlan] = {
    // 1. Check if we can use TungstenAggregate.
    val usesTungstenAggregate = TungstenAggregate.supportsAggregate(
      groupingExpressions,
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))

    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // 2. Create an Aggregate Operator for final aggregations.
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
    // The attributes of the final aggregation buffer, which is presented as input to the result
    // projection:
    val finalAggregateAttributes = finalAggregateExpressions.map {
      expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
    }

    val finalAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        nonCompleteAggregateAttributes = finalAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = pushedDownChild)
    } else {
      SortBasedAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        nonCompleteAggregateAttributes = finalAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = pushedDownChild)
    }
    finalAggregate :: Nil
  }
}
