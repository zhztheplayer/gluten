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
import org.apache.gluten.extension.joinagg.{ImplementJoinAggregate, JoinAggregateTags}
import org.apache.gluten.extension.joinagg.JoinAggregateFunctionWrapper.{FinalPhase, PartialMergePhase, PartialPhase}

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class ImplementJoinAggregateSuite extends PlanTest with SharedSparkSession {
  test("untagged aggregates are left to Spark") {
    withSQLConf(GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true") {
      val key = AttributeReference("key", IntegerType)()
      val agg = Aggregate(Seq(key), Seq(key), LocalRelation(Seq(key)))
      assert(ImplementJoinAggregate(spark)(agg).isEmpty)
    }
  }

  test("grouping-only aggregates follow their tagged phase") {
    for {
      phase <- Seq(PartialPhase, PartialMergePhase, FinalPhase)
      twoStageMode <- Seq("none", "final")
    } {
      withClue(s"phase=$phase, twoStageMode=$twoStageMode: ") {
        withSQLConf(
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
          GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_TWO_STAGE_MODE.key -> twoStageMode) {
          val key = AttributeReference("key", IntegerType)()
          val agg = Aggregate(
            Seq(key),
            Seq(Alias(key, "output_key")()),
            LocalRelation(Seq(key)))
          agg.setTagValue(JoinAggregateTags.phase, phase)

          val alternatives = ImplementJoinAggregate(spark)(agg)
          assert(alternatives.size == 1)
          val plan = alternatives.head
          val aggregates = plan.collect { case hash: HashAggregateExec => hash }
          assert(plan.output == agg.output)
          assert(aggregates.forall(_.aggregateExpressions.isEmpty))

          if (phase == FinalPhase) {
            assert(aggregates.head.requiredChildDistributionExpressions.contains(Seq(key)))
            val expectedStages = if (twoStageMode == "final") {
              2
            } else {
              1
            }
            assert(aggregates.size == expectedStages)
            assert(aggregates.tail.forall(_.requiredChildDistributionExpressions.isEmpty))
          } else {
            assert(aggregates.size == 1)
            assert(aggregates.head.requiredChildDistributionExpressions.isEmpty)
          }
        }
      }
    }
  }
}
