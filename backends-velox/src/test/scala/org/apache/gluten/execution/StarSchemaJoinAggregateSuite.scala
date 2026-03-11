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
    withTempView("ss_fact", "ss_sales", "ss_returns", "ss_dim") {
      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_fact AS
                  |SELECT
                  |  CAST(item_sk AS INT) AS item_sk,
                  |  CAST(cp_catalog_page_id AS INT) AS cp_catalog_page_id,
                  |  CAST(sales_price AS DECIMAL(12, 2)) AS sales_price,
                  |  CAST(return_amt AS DECIMAL(12, 2)) AS return_amt,
                  |  CAST(profit AS DECIMAL(12, 2)) AS profit,
                  |  CAST(net_loss AS DECIMAL(12, 2)) AS net_loss
                  |FROM VALUES
                  |  (1, 100, 10.00, 5.00, 7.00, 3.00),
                  |  (1, 100, 20.00, 8.00, 6.00, 2.00),
                  |  (2, 200, 30.00, 9.00, 4.00, 1.00)
                  |AS t(item_sk, cp_catalog_page_id, sales_price, return_amt, profit, net_loss)
                  |""".stripMargin)

      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_sales AS
                  |SELECT
                  |  item_sk,
                  |  cp_catalog_page_id,
                  |  sales_price,
                  |  profit
                  |FROM ss_fact
                  |""".stripMargin)

      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_returns AS
                  |SELECT
                  |  item_sk,
                  |  cp_catalog_page_id,
                  |  return_amt,
                  |  net_loss
                  |FROM ss_fact
                  |""".stripMargin)

      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_dim AS
                  |SELECT CAST(item_sk AS INT) AS item_sk
                  |FROM VALUES
                  |  (1), (1), (2), (2), (2)
                  |AS t(item_sk)
                  |""".stripMargin)

      val query =
        """
          |WITH base AS (
          |  SELECT
          |    s.item_sk,
          |    s.cp_catalog_page_id,
          |    s.sales_price,
          |    s.profit,
          |    CAST(0 AS DECIMAL(12, 2)) AS return_amt,
          |    CAST(0 AS DECIMAL(12, 2)) AS net_loss
          |  FROM ss_sales s
          |  UNION ALL
          |  SELECT
          |    r.item_sk,
          |    r.cp_catalog_page_id,
          |    CAST(0 AS DECIMAL(12, 2)) AS sales_price,
          |    CAST(0 AS DECIMAL(12, 2)) AS profit,
          |    r.return_amt,
          |    r.net_loss
          |  FROM ss_returns r
          |)
          |SELECT
          |  b.cp_catalog_page_id,
          |  SUM(b.sales_price) AS sum_sales_price,
          |  SUM(b.return_amt) AS sum_return_amt,
          |  SUM(b.profit) AS sum_profit,
          |  SUM(b.net_loss) AS sum_net_loss
          |FROM base b
          |JOIN ss_dim d
          |  ON b.item_sk = d.item_sk
          |GROUP BY b.cp_catalog_page_id
          |ORDER BY b.cp_catalog_page_id
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
