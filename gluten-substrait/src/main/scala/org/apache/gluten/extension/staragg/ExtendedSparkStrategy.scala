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

package org.apache.gluten.extension.staragg

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{PlanLater, SparkPlan, SparkPlanner, SparkStrategies, SparkStrategy}

case class ExtendedSparkStrategy(delegate: SparkStrategy, postRule: Rule[SparkPlan]) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val plans = delegate.apply(plan)
    val out = plans.map {
      case later: PlanLater => later
      case other => postRule.apply(other)
    }
    out
  }
}

object ExtendedSparkStrategy {
  def templateStrategies(): SparkStrategies = new SparkPlanner(null, null)
}