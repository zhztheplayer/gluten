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
package org.apache.gluten.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}

/**
 * Prototype rule for unwrapping StarSchemaAggregateWrapper at SparkPlan phase.
 *
 * This rule is intentionally conservative and is not injected yet. It only rewrites wrapper forms
 * that can be translated to native Spark aggregate modes without introducing additional
 * projection/extraction nodes.
 */
case class UnwrapStarSchemaAggregate(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val out = plan.transformUp {
      case agg: HashAggregateExec =>
        val rewritten = rewriteAggregateExpressions(agg.aggregateExpressions)
        if (rewritten == agg.aggregateExpressions) {
          agg
        } else {
          agg.copy(aggregateExpressions = rewritten)
        }
      case agg: ObjectHashAggregateExec =>
        val rewritten = rewriteAggregateExpressions(agg.aggregateExpressions)
        if (rewritten == agg.aggregateExpressions) {
          agg
        } else {
          agg.copy(aggregateExpressions = rewritten)
        }
      case agg: SortAggregateExec =>
        val rewritten = rewriteAggregateExpressions(agg.aggregateExpressions)
        if (rewritten == agg.aggregateExpressions) {
          agg
        } else {
          agg.copy(aggregateExpressions = rewritten)
        }
    }
    out
  }

  private def rewriteAggregateExpressions(
      aggregateExpressions: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    aggregateExpressions.map(rewriteAggregateExpression)
  }

  private def rewriteAggregateExpression(
      aggregateExpression: AggregateExpression): AggregateExpression = {
    aggregateExpression.aggregateFunction match {
      case wrapper: StarSchemaAggregateWrapper if canUnwrap(wrapper, aggregateExpression) =>
        aggregateExpression.copy(
          aggregateFunction = wrapper.innerAgg,
          mode =
            StarSchemaAggregateWrapper.semanticMode(aggregateExpression.mode, wrapper.targetPhase))
      case _ =>
        aggregateExpression
    }
  }

  private def canUnwrap(
      wrapper: StarSchemaAggregateWrapper,
      aggregateExpression: AggregateExpression): Boolean = {
    wrapper.targetPhase match {
      case StarSchemaAggregateWrapper.PartialPhase => true
      case StarSchemaAggregateWrapper.FinalPhase => aggregateExpression.mode == Final
    }
  }
}
