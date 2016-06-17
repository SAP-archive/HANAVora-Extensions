/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.util._

/**
 * Provides helper methods for comparing plans.
 *
 * Based on Spark's PlanTest. The original class is internal to Spark's tests
 * and backwards compatibility is not maintained across bugfix releases (e.g. 1.4.0 -> 1.4.1).
 */
object PlanComparisonUtils {

  /**
   * Since attribute references are given globally unique IDs during analysis,
   * we have to normalize them to check if two different queries are identical.
   */
  def normalizeExprIds(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
    }
  }

  /**
   * Compares two [[LogicalPlan]]s without taking the IDs of their [[Expression]]s
    * into account.
   *
   * @param plan1 The first plan.
   * @param plan2 The second plan.
   * @return [[None]] if the plans match each other, or [[Some]] with
   *         information about differences if the plans do not match.
   */
  def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan): Option[String] = {
    val normalized1 = normalizeExprIds(plan1)
    val normalized2 = normalizeExprIds(plan2)
    if (normalized1 != normalized2) {
      Some(
        s"""
           |== FAIL: Plans do not match ===
           |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
         """.stripMargin)
    } else None
  }

  /**
   * Compares two [[Expression]]s without taking their IDs into account.
   *
   * @param e1 The first expression.
   * @param e2 The second expression.
   * @return [[None]] if the expressions match each other, or [[Some]] with
   *         information about differences if the expressions do not match.
   */
  def compareExpressions(e1: Expression, e2: Expression): Option[String] =
    comparePlans(Filter(e1, OneRowRelation), Filter(e2, OneRowRelation))

}
