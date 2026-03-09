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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Sum}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

class StarSchemaPreAggregateSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    override val batches: Seq[Batch] = Seq(
      // TODO
    )
  }

  test("pre-aggregate store_sales by foreign keys before joining date_dim and item") {
    val ss_item_sk = AttributeReference("ss_item_sk", IntegerType)()
    val ss_sold_date_sk = AttributeReference("ss_sold_date_sk", IntegerType)()
    val d_date_sk = AttributeReference("d_date_sk", IntegerType)()
    val d_year = AttributeReference("d_year", IntegerType)()
    val d_date = AttributeReference("d_date", DateType)()
    val i_item_sk = AttributeReference("i_item_sk", IntegerType)()
    val i_item_desc = AttributeReference("i_item_desc", StringType)()

    val storeSales =
      LocalRelation(ss_item_sk, ss_sold_date_sk)

    val dateDim =
      LocalRelation(d_date_sk, d_year, d_date)

    val item =
      LocalRelation(i_item_sk, i_item_desc)

    val join1 =
      Join(storeSales, dateDim, Inner, Some(ss_sold_date_sk === d_date_sk), JoinHint.NONE)

    val join2 =
      Join(join1, item, Inner, Some(ss_item_sk === i_item_sk), JoinHint.NONE)

    val itemDescExpr =
      Alias(Substring(i_item_desc, Literal(1), Literal(30)), "itemdesc")()

    val itemSkExpr =
      Alias(i_item_sk, "item_sk")()

    val soldDateExpr =
      Alias(d_date, "solddate")()

    val cntExpr =
      Alias(Count(Seq(Literal(1))).toAggregateExpression(), "cnt")()

    val original =
      Aggregate(
        Seq(Substring(i_item_desc, Literal(1), Literal(30)), i_item_sk, d_date),
        Seq(itemDescExpr, itemSkExpr, soldDateExpr, cntExpr),
        Filter(In(d_year, Seq(Literal(1999), Literal(2000), Literal(2001), Literal(2002))), join2)
      )

    val preAgg =
      Aggregate(
        Seq(ss_item_sk, ss_sold_date_sk),
        Seq(
          ss_item_sk,
          ss_sold_date_sk,
          Alias(Count(Seq(Literal(1))).toAggregateExpression(), "partial_cnt")()),
        storeSales)
    val partialCnt = preAgg.output.find(_.name == "partial_cnt").get

    val rewrittenJoin1 =
      Join(preAgg, dateDim, Inner, Some(ss_sold_date_sk === d_date_sk), JoinHint.NONE)

    val rewrittenJoin2 =
      Join(rewrittenJoin1, item, Inner, Some(ss_item_sk === i_item_sk), JoinHint.NONE)

    val expected =
      Aggregate(
        Seq(Substring(i_item_desc, Literal(1), Literal(30)), i_item_sk, d_date),
        Seq(
          Alias(Substring(i_item_desc, Literal(1), Literal(30)), "itemdesc")(),
          Alias(i_item_sk, "item_sk")(),
          Alias(d_date, "solddate")(),
          Alias(Sum(partialCnt).toAggregateExpression(), "cnt")()
        ),
        Filter(
          In(d_year, Seq(Literal(1999), Literal(2000), Literal(2001), Literal(2002))),
          rewrittenJoin2)
      )

    comparePlans(Optimize.execute(original.analyze), expected.analyze)
  }
}
