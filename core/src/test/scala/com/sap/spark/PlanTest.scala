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

package com.sap.spark

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.util.PlanComparisonUtils
import org.scalatest.Suite

/**
 * Provides helper methods for comparing plans.
 *
 * Based on Spark's PlanTest. The original class is internal to Spark's tests
 * and backwards compatibility is not maintained across bugfix releases (e.g. 1.4.0 -> 1.4.1).
 */
trait PlanTest {
  self: Suite =>

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) =
    PlanComparisonUtils.comparePlans(plan1, plan2)  match {
      case Some(msg) => fail(msg)
      case None =>
    }

  /** Fails the test if the two expressions do not match */
  protected def compareExpressions(e1: Expression, e2: Expression): Unit = {
    comparePlans(Filter(e1, OneRowRelation), Filter(e2, OneRowRelation))
  }

}
