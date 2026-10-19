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
package org.apache.gluten.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.joinagg.{JoinAggregateFunctionWrapper, PushAggregateThroughJoin}

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo, GreaterThan, Literal, Rand}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LocalRelation, Project}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class PushAggregateThroughJoinRuleSuite extends PlanTest with SharedSparkSession {
  private val joinAggregateRule = PushAggregateThroughJoin(spark)

  test("pushPartialAggToJoinSide accepts only an eligible side") {
    val leftKey = AttributeReference("left_key", IntegerType)()
    val value = AttributeReference("value", IntegerType)()
    val rightKey = AttributeReference("right_key", IntegerType)()
    val join = Join(
      LocalRelation(Seq(leftKey, value)),
      LocalRelation(Seq(rightKey)),
      Inner,
      Some(EqualTo(leftKey, rightKey)),
      JoinHint.NONE)
    val leftPartialSum = Alias(
      JoinAggregateFunctionWrapper.wrapperPartial(Sum(value)).toAggregateExpression(),
      "left_partial_sum")()
    val rightPartialSum = Alias(
      JoinAggregateFunctionWrapper.wrapperPartial(Sum(rightKey)).toAggregateExpression(),
      "right_partial_sum")()

    val leftPushed = joinAggregateRule.pushPartialAggToJoinSide(
      Seq(leftKey),
      Seq(leftKey, leftPartialSum),
      join,
      Nil,
      joinAggregateRule.JoinLeft)
    assert(leftPushed.exists(_.child eq join.left))
    assert(leftPushed.get.aggregateExpressions.last == leftPartialSum)
    assert(joinAggregateRule
      .pushPartialAggToJoinSide(
        Seq(leftKey),
        Seq(leftKey, leftPartialSum),
        join,
        Nil,
        joinAggregateRule.JoinRight)
      .isEmpty)
    val rightPushed = joinAggregateRule.pushPartialAggToJoinSide(
      Seq(leftKey),
      Seq(leftKey, rightPartialSum),
      join,
      Nil,
      joinAggregateRule.JoinRight)
    assert(rightPushed.exists(_.child eq join.right))
  }

  test("pushPartialAggToJoinSide supports grouping-only pushdown on either side") {
    val leftKey = AttributeReference("left_key", IntegerType)()
    val rightKey = AttributeReference("right_key", IntegerType)()
    val join = Join(
      LocalRelation(Seq(leftKey)),
      LocalRelation(Seq(rightKey)),
      Inner,
      Some(EqualTo(leftKey, rightKey)),
      JoinHint.NONE)
    val keys = Seq(leftKey, rightKey)

    Seq(joinAggregateRule.JoinLeft, joinAggregateRule.JoinRight).foreach {
      side =>
        val pushed = joinAggregateRule
          .pushPartialAggToJoinSide(keys, keys, join, Nil, side)
          .get
        assert(pushed.child eq side.plan(join))
        assert(pushed.groupingExpressions == side.plan(join).output)
        assert(pushed.aggregateExpressions == side.plan(join).output)
    }
  }

  test("pushPartialAggToJoinSide grouping-only pushdown is idempotent") {
    val leftKey = AttributeReference("left_key", IntegerType)()
    val rightKey = AttributeReference("right_key", IntegerType)()
    val join = Join(
      LocalRelation(Seq(leftKey)),
      LocalRelation(Seq(rightKey)),
      Inner,
      Some(EqualTo(leftKey, rightKey)),
      JoinHint.NONE)
    val keys = Seq(leftKey, rightKey)

    Seq(joinAggregateRule.JoinLeft, joinAggregateRule.JoinRight).foreach {
      side =>
        val pushed = joinAggregateRule
          .pushPartialAggToJoinSide(keys, keys, join, Nil, side)
          .get
        val pushedJoin = side.replace(join, pushed)
        assert(joinAggregateRule
          .pushPartialAggToJoinSide(keys, keys, pushedJoin, Nil, side)
          .isEmpty)
    }
  }

  test("grouping-only pushdown rule is idempotent") {
    Seq(false, true).foreach {
      partialMergeEnabled =>
        withSQLConf(
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MAX_DEPTH.key -> "1",
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_PARTIAL_MERGE_ENABLED.key ->
            partialMergeEnabled.toString
        ) {
          val leftKey = AttributeReference("left_key", IntegerType)()
          val rightKey = AttributeReference("right_key", IntegerType)()
          val join = Join(
            LocalRelation(Seq(leftKey)),
            LocalRelation(Seq(rightKey)),
            Inner,
            Some(EqualTo(leftKey, rightKey)),
            JoinHint.NONE)
          val keys = Seq(leftKey, rightKey)
          val plan = Aggregate(
            keys,
            Seq(Alias(leftKey, "left_output")(), Alias(rightKey, "right_output")()),
            Project(keys, join))
          val rule = PushAggregateThroughJoin(spark)

          val once = rule(plan)
          assert(once.collect { case _: Aggregate => 1 }.size == 3)
          assert(once.output == plan.output)
          assert(once.resolved)
          assert(once.collect { case p if p.missingInput.nonEmpty => p }.isEmpty)
          assert(rule.getSuccessfulPushCount == 2)

          // The second run tries to split every DISTINCT:
          // - Left/right DISTINCT: no join below them, so no push succeeds.
          // - Top DISTINCT: both join sides are already grouped.
          // A single join has no between-join merge in either option setting.
          // No push succeeds, so every temporary split is discarded.
          val twice = rule(once)
          assert(twice eq once)
          assert(rule.getSuccessfulPushCount == 2)
        }
    }
  }

  test("grouping-only pushdown keeps nondeterministic expressions above the join") {
    withSQLConf(
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MAX_DEPTH.key -> "1") {
      val leftKey = AttributeReference("left_key", IntegerType)()
      val rightKey = AttributeReference("right_key", IntegerType)()
      val join = Join(
        LocalRelation(Seq(leftKey)),
        LocalRelation(Seq(rightKey)),
        Inner,
        Some(EqualTo(leftKey, rightKey)),
        JoinHint.NONE)
      val keys = Seq(leftKey, rightKey)
      val randomValue = Alias(Rand(0L), "random_value")()
      val children = Seq(
        Project(keys :+ randomValue, join),
        Filter(GreaterThan(Rand(0L), Literal(0.5)), join))
      val rule = PushAggregateThroughJoin(spark)

      children.foreach {
        child =>
          val plan = Aggregate(child.output, child.output, child)
          assert(rule(plan) eq plan)
      }
    }
  }
}
