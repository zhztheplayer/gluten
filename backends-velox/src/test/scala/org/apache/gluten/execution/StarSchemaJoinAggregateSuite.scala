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

  test("Minimal pushdown for two different decimal sums") {
    withTempView("ss_fact", "ss_dim") {
      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_fact AS
                  |SELECT
                  |  CAST(item_sk AS INT) AS item_sk,
                  |  CAST(cp_catalog_page_id AS INT) AS grp_id,
                  |  CAST(sales_price AS DECIMAL(12, 2)) AS dec_a,
                  |  CAST(return_amt AS DECIMAL(12, 2)) AS dec_b
                  |FROM VALUES
                  |  (1, 100, 10.00, 5.00),
                  |  (1, 100, 20.00, 8.00),
                  |  (2, 200, 30.00, 9.00)
                  |AS t(item_sk, cp_catalog_page_id, sales_price, return_amt)
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
          |SELECT
          |  f.grp_id,
          |  SUM(f.dec_a) AS sum_dec_a,
          |  SUM(f.dec_b) AS sum_dec_b
          |FROM ss_fact f
          |JOIN ss_dim d
          |  ON f.item_sk = d.item_sk
          |GROUP BY f.grp_id
          |ORDER BY f.grp_id
          |""".stripMargin

      withSQLConf(
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.shuffle.partitions" -> "100",
        "spark.sql.autoBroadcastJoinThreshold" -> "10m") {
        runQueryAndCompare(query) {
          df =>
            assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
            checkGlutenPlan[HashAggregateExecTransformer](df)
        }
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
