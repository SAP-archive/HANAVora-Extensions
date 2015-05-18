/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Implicits._
import org.apache.spark.sql.catalyst.planning.{PartialAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{expressions => expr}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, Strategy, execution, sources => src}

/**
 * Strategy to push down aggregates to a DataSource when they are supported.
 */
private[sql] object PushDownAggregatesStrategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case partialAgg@PartialAggregation(
    finalGroupingAttributes,
    finalAggregateExpressions,
    partialGroupingExpressions,
    partialAggregateExpressions,
    child@PhysicalOperation(projectList, filters,
    logicalRelation@LogicalRelation(t: PrunedFilteredAggregatedScan))) => partialAgg match {
      case composedPartialAgg@ComposedPartialAggregation(finalGroupingAttributes,
      finalAggregateExpressions,
      partialGroupingExpressions,
      partialAggregateExpressions,
      dataSourceGroupingExpressions,
      dataSourceAggregateExpressions,
      child@PhysicalOperation(projectList, filters,
      logicalRelation@LogicalRelation(t: PrunedFilteredAggregatedScan))) =>
        Seq(execution.Aggregate(
          partial = false,
          finalGroupingAttributes,
          finalAggregateExpressions,
          execution.Aggregate(
            partial = true,
            partialGroupingExpressions,
            partialAggregateExpressions,
            pruneFilterProjectAgg(logicalRelation,
              projectList,
              filters,
              dataSourceGroupingExpressions,
              dataSourceAggregateExpressions,
              (a, f, _, _) => t.buildScanAggregate(a, f, dataSourceGroupingExpressions,
                dataSourceAggregateExpressions)
            ))
        ))
      case _ =>
        Seq(execution.Aggregate(
          partial = false,
          finalGroupingAttributes,
          finalAggregateExpressions,
          pruneFilterProjectAgg(logicalRelation,
            projectList,
            filters,
            partialGroupingExpressions,
            partialAggregateExpressions,
            (a, f, _, _) => t.buildScanAggregate(a, f, partialGroupingExpressions,
              partialAggregateExpressions)
          ))
        )
    }
    case _ => Nil
  }

  // Based on Public API.
  protected def pruneFilterProjectAgg(
                                       relation: LogicalRelation,
                                       projectList: Seq[NamedExpression],
                                       filterPredicates: Seq[Expression],
                                       groupingExpressions: Seq[Expression],
                                       aggregateExpressions: Seq[NamedExpression],
                                       scanBuilder: (
                                         Array[String],
                                           Array[Filter],
                                           Seq[Expression],
                                           Seq[NamedExpression]) => RDD[Row]) = {
    pruneFilterProjectAggRaw(
      relation,
      projectList,
      filterPredicates,
      groupingExpressions,
      aggregateExpressions,
      (requestedColumns, pushedFilters, ge, pc) => {
        scanBuilder(requestedColumns.map(_.name).toArray,
          selectFilters(pushedFilters).toArray, ge, pc)
      })
  }

  // Based on Catalyst expressions.
  protected def pruneFilterProjectAggRaw(
                                          relation: LogicalRelation,
                                          projectList: Seq[NamedExpression],
                                          filterPredicates: Seq[Expression],
                                          groupingExpressions: Seq[Expression],
                                          aggregateExpressions: Seq[NamedExpression],
                                          scanBuilder:
                                          (Seq[Attribute],
                                            Seq[Expression],
                                            Seq[Expression],
                                            Seq[NamedExpression]) => RDD[Row]) = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val pushedFilters = filterPredicates.map {
      _ transform {
        case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
      }
    }

    if (projectList.map(_.toAttribute) == projectList &&
      projectSet.size == projectList.size &&
      filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns =
        projectList.asInstanceOf[Seq[Attribute]] // Safe due to if above.
          .map(relation.attributeMap) ++ // Match original case of attributes.
          aggregateExpressions.filter(_.isInstanceOf[Alias]).map(_.toAttribute)

      execution.PhysicalRDD(
        aggregateExpressions.map(_.toAttribute),
        scanBuilder(requestedColumns, pushedFilters, groupingExpressions, aggregateExpressions))
    } else {
      val requestedColumns = (projectSet ++ filterSet).map(relation.attributeMap).toSeq

      execution.PhysicalRDD(aggregateExpressions.map(_.toAttribute),
        scanBuilder(requestedColumns, pushedFilters, groupingExpressions, aggregateExpressions))
    }
  }

  /**
   * Selects Catalyst predicate [[Expression]]s which are convertible into data source [[Filter]]s,
   * and convert them.
   */
  protected[sql] def selectFilters(filters: Seq[Expression]) =
    filters.flatMap(expressionToFilter)

  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
  private def expressionToFilter(predicate: Expression): Option[Filter] =
    predicate match {
      case expr.EqualTo(a: Attribute, Literal(v, _)) => Some(src.EqualTo(a.name, v))
      case expr.EqualTo(Literal(v, _), a: Attribute) => Some(src.EqualTo(a.name, v))
      case expr.GreaterThan(a: Attribute, Literal(v, _)) => Some(src.GreaterThan(a.name, v))
      case expr.GreaterThan(Literal(v, _), a: Attribute) => Some(src.LessThan(a.name, v))
      case expr.LessThan(a: Attribute, Literal(v, _)) => Some(src.LessThan(a.name, v))
      case expr.LessThan(Literal(v, _), a: Attribute) => Some(src.GreaterThan(a.name, v))
      case expr.GreaterThanOrEqual(a: Attribute, Literal(v, _)) =>
        Some(src.GreaterThanOrEqual(a.name, v))
      case expr.GreaterThanOrEqual(Literal(v, _), a: Attribute) =>
        Some(src.LessThanOrEqual(a.name, v))
      case expr.LessThanOrEqual(a: Attribute, Literal(v, _)) =>
        Some(src.LessThanOrEqual(a.name, v))
      case expr.LessThanOrEqual(Literal(v, _), a: Attribute) =>
        Some(src.GreaterThanOrEqual(a.name, v))
      case expr.InSet(a: Attribute, set) => Some(src.In(a.name, set.toArray))
      case expr.IsNull(a: Attribute) => Some(src.IsNull(a.name))
      case expr.IsNotNull(a: Attribute) => Some(src.IsNotNull(a.name))
      case expr.And(left, right) =>
        (expressionToFilter(left) ++ expressionToFilter(right)).reduceOption(src.And)
      case expr.Or(left, right) =>
        for {
          leftFilter <- expressionToFilter(left)
          rightFilter <- expressionToFilter(right)
        } yield src.Or(leftFilter, rightFilter)

      case expr.Not(child) => expressionToFilter(child).map(src.Not)
      case _ => None
    }
}

/**
 * Matches a logical aggregation that can be performed on distributed data in three steps:
 *  - The first one delegates some operations to the data source.
 *  - The second operates on the data in each partition performing partial
 *  aggregation that couldn't be carried out by the data source for each group.
 *  - The third occurs after the shuffle and completes the aggregation.
 *
 * This pattern will only match if all aggregate expressions can be computed partially and will
 * return the rewritten aggregation expressions for every phase.
 *
 * The returned values for this match are as follows:
 *  - Grouping attributes for the final aggregation.
 *  - Aggregates for the final aggregation.
 *  - Grouping expressions for the partial aggregation.
 *  - Partial aggregate expressions.
 *  - Data source grouping expressions.
 *  - Data source aggregate expressions.
 *  - Input to the aggregation.
 */
private object ComposedPartialAggregation {
  type ReturnType =
  (Seq[Attribute], Seq[NamedExpression], Seq[Expression], Seq[NamedExpression],
    Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case partialAgg@PartialAggregation(
    namedGroupingAttributes,
    rewrittenAggregateExpressions,
    groupingExpressions,
    partialComputation,
    child@PhysicalOperation(projectList, filters,
    logicalRelation@LogicalRelation(t: PrunedFilteredAggregatedScan))) =>

      /* Splitting the data depending on data source support */
      val (supportedByDataSource, notSupportedByDataSource) =
        partialComputation.partition(t.supports)

      if (!notSupportedByDataSource.isEmpty) {
        val dataSourceExtra: Seq[NamedExpression] =
          notSupportedByDataSource.flatMap(_.extractAttributes)
        val dataSourceSupportedPartialAgg = supportedByDataSource ++ dataSourceExtra

        /* Adding the extra attributes needed during the partial aggregation phase */
        val noDataSourceExtra: Seq[NamedExpression] =
          supportedByDataSource.map(_.toAttribute)
        val dataSourceNotSupportedPartialAgg = notSupportedByDataSource ++ noDataSourceExtra
        val dataSourceGroupingExpressions = (groupingExpressions ++ dataSourceExtra).distinct.
          filterNot(exp => notSupportedByDataSource.exists(containsAttributeReference(exp)))

        val filteredGroupingExpressions = groupingExpressions.filterNot(
          dataSourceGroupingExpressions.contains)
        val partialGroupingExpressions = (filteredGroupingExpressions ++ noDataSourceExtra).distinct

        Some(
          (namedGroupingAttributes,
            rewrittenAggregateExpressions,
            partialGroupingExpressions,
            dataSourceNotSupportedPartialAgg,
            dataSourceGroupingExpressions,
            dataSourceSupportedPartialAgg,
            child))
      } else {
        None
      }
    case _ => None
  }

  private def containsAttributeReference(expression: Expression)
                                        (notSupportedExpression: Expression): Boolean =
    notSupportedExpression match {
      case alias: Alias => alias.child == expression
      case attr: Expression => attr equals expression
    }
}
