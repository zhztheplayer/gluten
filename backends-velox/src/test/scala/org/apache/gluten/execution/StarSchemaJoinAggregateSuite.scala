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

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.Decimal

class StarSchemaJoinAggregateSuite extends VeloxTPCHTableSupport {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "true")
  }

  test("star-schema wrapper aggregate") {
    val query =
      """
        |SELECT
        |  SUM(l_extendedprice * (1 - l_discount)) AS revenue
        |FROM lineitem
        |JOIN part
        |  ON l_partkey = p_partkey
        |WHERE p_size > 10 AND l_shipmode IN ('AIR', 'RAIL')
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
        checkGlutenPlan[HashAggregateExecTransformer](df)
    }
  }

  test("Support star-schema wrapper aggregate for simplified TPC-H q12") {
    val query =
      """
        |SELECT
        |  l_shipmode,
        |  SUM(CASE
        |    WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
        |    THEN 1
        |    ELSE 0
        |  END) AS high_line_count,
        |  SUM(CASE
        |    WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
        |    THEN 1
        |    ELSE 0
        |  END) AS low_line_count
        |FROM orders
        |JOIN lineitem
        |  ON o_orderkey = l_orderkey
        |WHERE l_shipmode IN ('MAIL', 'SHIP')
        |  AND l_commitdate < l_receiptdate
        |  AND l_shipdate < l_commitdate
        |  AND l_receiptdate >= DATE '1994-01-01'
        |  AND l_receiptdate < DATE '1995-01-01'
        |GROUP BY l_shipmode
        |ORDER BY l_shipmode
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
        checkGlutenPlan[HashAggregateExecTransformer](df)
    }
  }

  test("Support star-schema wrapper aggregate for multi-field agg buffer (avg)") {
    val query =
      """
        |SELECT
        |  c_nationkey,
        |  AVG(CAST(o_shippriority AS DOUBLE)) AS avg_shippriority
        |FROM customer
        |JOIN orders
        |  ON c_custkey = o_custkey
        |WHERE c_mktsegment = 'BUILDING'
        |GROUP BY c_nationkey
        |ORDER BY c_nationkey
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
        checkGlutenPlan[HashAggregateExecTransformer](df)
    }
  }

  test("Support star-schema wrapper aggregate with duplicate decimal sum buffers") {
    val query =
      """
        |SELECT
        |  c_nationkey,
        |  SUM(o_totalprice) AS sum_totalprice_a,
        |  SUM(o_totalprice + CAST(1 AS DECIMAL(12, 2))) AS sum_totalprice_b,
        |  SUM(CAST(o_shippriority AS BIGINT)) AS sum_shippriority,
        |  SUM(CAST(o_orderkey AS BIGINT)) AS sum_orderkey
        |FROM customer
        |JOIN orders
        |  ON c_custkey = o_custkey
        |WHERE c_mktsegment = 'BUILDING'
        |GROUP BY c_nationkey
        |ORDER BY c_nationkey
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
        checkGlutenPlan[HashAggregateExecTransformer](df)
    }
  }

  test("Reproduce duplicate sum/isEmpty remap assertion") {
    withTempView("ss_fact", "ss_dim") {
      import spark.implicits._

      Seq(
        (1, 100, Decimal(10), Decimal(5), Decimal(7), Decimal(3)),
        (1, 100, Decimal(20), Decimal(8), Decimal(6), Decimal(2)),
        (2, 200, Decimal(30), Decimal(9), Decimal(4), Decimal(1))
      ).toDF("item_sk", "cp_catalog_page_id", "sales_price", "return_amt", "profit", "net_loss")
        .selectExpr(
          "cast(item_sk as int) as item_sk",
          "cast(cp_catalog_page_id as int) as cp_catalog_page_id",
          "cast(sales_price as decimal(12,2)) as sales_price",
          "cast(return_amt as decimal(12,2)) as return_amt",
          "cast(profit as decimal(12,2)) as profit",
          "cast(net_loss as decimal(12,2)) as net_loss")
        .createOrReplaceTempView("ss_fact")

      Seq((1), (1), (2), (2), (2))
        .toDF("item_sk")
        .selectExpr("cast(item_sk as int) as item_sk")
        .createOrReplaceTempView("ss_dim")

      val query =
        """
          |SELECT
          |  f.cp_catalog_page_id,
          |  SUM(f.sales_price) AS sum_sales_price,
          |  SUM(f.return_amt) AS sum_return_amt,
          |  SUM(f.profit) AS sum_profit,
          |  SUM(f.net_loss) AS sum_net_loss
          |FROM ss_fact f
          |JOIN ss_dim d
          |  ON f.item_sk = d.item_sk
          |GROUP BY f.cp_catalog_page_id
          |ORDER BY f.cp_catalog_page_id
          |""".stripMargin

      runQueryAndCompare(query) {
        df =>
          assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
          checkGlutenPlan[HashAggregateExecTransformer](df)
      }
    }
  }

  test("Support star-schema wrapper aggregate for simplified TPC-H q18") {
    val query =
      """
        |SELECT
        |  c_name,
        |  c_custkey,
        |  o_orderkey,
        |  o_orderdate,
        |  o_totalprice,
        |  SUM(l_quantity)
        |FROM customer, orders, lineitem
        |WHERE o_orderkey IN (
        |  SELECT
        |    l_orderkey
        |  FROM lineitem
        |  GROUP BY l_orderkey
        |  HAVING SUM(l_quantity) > 300
        |)
        |  AND c_custkey = o_custkey
        |  AND o_orderkey = l_orderkey
        |GROUP BY
        |  c_name,
        |  c_custkey,
        |  o_orderkey,
        |  o_orderdate,
        |  o_totalprice
        |ORDER BY
        |  o_totalprice DESC,
        |  o_orderdate
        |LIMIT 100
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
        checkGlutenPlan[HashAggregateExecTransformer](df)
    }
  }
}
