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
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, _}
import org.apache.spark.sql.catalyst.planning.{PartialAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, Strategy, execution, sources}

/**
 * Strategy to push down aggregates to a DataSource when they are supported.
 */
private[sql] object PushDownAggregatesStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case partialAgg @ PartialAggregation(
    namedGroupingAttributes,
    rewrittenAggregateExpressions,
    groupingExpressions,
    partialComputation,
    child@PhysicalOperation(projectList, filters,
    l@LogicalRelation(t: PrunedFilteredAggregatedScan))) =>
      execution.Aggregate(
        partial = false,
        namedGroupingAttributes,
        rewrittenAggregateExpressions,
        pruneFilterProjectAgg(l,
          projectList,
          filters,
          groupingExpressions,
          partialComputation,
          (a, f, ge, pc) => t.buildScanAggregate(a, f, groupingExpressions, partialComputation)
        )
      ) :: Nil
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
    val filterCondition = filterPredicates.reduceLeftOption(expressions.And)

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
  protected[sql] def selectFilters(filters: Seq[Expression]) = {
    def translate(predicate: Expression): Option[Filter] = predicate match {
      case expressions.EqualTo(a: Attribute, Literal(v, _)) =>
        Some(sources.EqualTo(a.name, v))
      case expressions.EqualTo(Literal(v, _), a: Attribute) =>
        Some(sources.EqualTo(a.name, v))

      case expressions.GreaterThan(a: Attribute, Literal(v, _)) =>
        Some(sources.GreaterThan(a.name, v))
      case expressions.GreaterThan(Literal(v, _), a: Attribute) =>
        Some(sources.LessThan(a.name, v))

      case expressions.LessThan(a: Attribute, Literal(v, _)) =>
        Some(sources.LessThan(a.name, v))
      case expressions.LessThan(Literal(v, _), a: Attribute) =>
        Some(sources.GreaterThan(a.name, v))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, _)) =>
        Some(sources.GreaterThanOrEqual(a.name, v))
      case expressions.GreaterThanOrEqual(Literal(v, _), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, v))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, _)) =>
        Some(sources.LessThanOrEqual(a.name, v))
      case expressions.LessThanOrEqual(Literal(v, _), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, v))

      case expressions.InSet(a: Attribute, set) =>
        Some(sources.In(a.name, set.toArray))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        (translate(left) ++ translate(right)).reduceOption(sources.And)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translate(left)
          rightFilter <- translate(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translate(child).map(sources.Not)

      case _ => None
    }

    filters.flatMap(translate)
  }
}
