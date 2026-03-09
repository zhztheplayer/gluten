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

import org.apache.gluten.extension.StarSchemaPreAggregateRule

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

class StarSchemaPreAggregateSuite extends PlanTest {
  private val starSchemaRule = StarSchemaPreAggregateRule(null)
  private val ss_item_sk = AttributeReference("ss_item_sk", IntegerType)()
  private val ss_sold_date_sk = AttributeReference("ss_sold_date_sk", IntegerType)()
  private val d_date_sk = AttributeReference("d_date_sk", IntegerType)()
  private val d_year = AttributeReference("d_year", IntegerType)()
  private val d_date = AttributeReference("d_date", DateType)()
  private val i_item_sk = AttributeReference("i_item_sk", IntegerType)()
  private val i_item_desc = AttributeReference("i_item_desc", StringType)()

  private val storeSales =
    LocalRelation(ss_item_sk, ss_sold_date_sk)

  private val dateDim =
    LocalRelation(d_date_sk, d_year, d_date)

  private val item =
    LocalRelation(i_item_sk, i_item_desc)

  private val localTables: Map[String, LocalRelation] = Map(
    "store_sales" -> storeSales,
    "date_dim" -> dateDim,
    "item" -> item
  )

  private def parseSqlWithLocalTables(sqlText: String): LogicalPlan = {
    CatalystSqlParser.parsePlan(sqlText).transformDown {
      case relation: UnresolvedRelation =>
        localTables.getOrElse(relation.tableName, relation)
    }
  }

  private object Optimize extends RuleExecutor[LogicalPlan] {
    override val batches: Seq[Batch] = Seq(
      Batch("StarSchemaPreAggregate", FixedPoint(10), starSchemaRule)
    )
  }

  private case class PushdownCase(inputSql: String, expectedPushCount: Int, expectedAggCount: Int)

  private def runCase(testCase: PushdownCase): Unit = {
    val original = parseSqlWithLocalTables(testCase.inputSql)

    starSchemaRule.resetSuccessfulPushCount()
    val optimized = Optimize.execute(original.analyze)
    val aggregateNodeCount = optimized.collect { case _: Aggregate => 1 }.size
    val nodesWithMissingInput = optimized.collect {
      case p if p.missingInput.nonEmpty => p
    }

    assert(optimized.resolved)
    assert(
      nodesWithMissingInput.isEmpty,
      s"Plan has missing input:\n${nodesWithMissingInput.map(_.treeString).mkString("\n---\n")}")
    assert(starSchemaRule.getSuccessfulPushCount == testCase.expectedPushCount)
    assert(aggregateNodeCount == testCase.expectedAggCount)
  }

  test("pre-aggregate store_sales for both joins with having filter") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  substring(i_item_desc, 1, 30) AS itemdesc,
                   |  i_item_sk AS item_sk,
                   |  d_date AS solddate,
                   |  count(1) AS cnt
                   |FROM store_sales
                   |JOIN date_dim ON ss_sold_date_sk = d_date_sk
                   |JOIN item ON ss_item_sk = i_item_sk
                   |WHERE d_year IN (1999, 2000, 2001, 2002)
                   |GROUP BY substring(i_item_desc, 1, 30), i_item_sk, d_date
                   |HAVING count(1) > 4
                   |""".stripMargin,
      expectedPushCount = 4,
      expectedAggCount = 5
    )
    runCase(pushdownCase)
  }
}
