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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class StarSchemaJoinAggregateSuite extends VeloxTPCHTableSupport with AdaptiveSparkPlanHelper {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "true")
      .set(GlutenConfig.ENABLE_JOIN_AGGREGATE_RULES.key, "true")
  }

  private val q5TempTables = Seq(
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
    "date_dim",
    "store",
    "catalog_page",
    "customer_address",
    "call_center",
    "web_site",
    "item",
    "promotion",
    "customer_demographics"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    createQ5MiniTables()
  }

  override def afterAll(): Unit = {
    q5TempTables.foreach {
      t =>
        if (spark.catalog.tableExists(t)) {
          spark.catalog.dropTempView(t)
        }
    }
    super.afterAll()
  }

  private def createQ5MiniTables(): Unit = {
    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW date_dim AS
                |SELECT CAST(d_date_sk AS BIGINT) AS d_date_sk,
                |       CAST(d_date AS DATE) AS d_date,
                |       CAST(d_year AS INT) AS d_year
                |FROM VALUES
                |  (1, DATE'1998-08-05', 1998),
                |  (2, DATE'1998-08-06', 1998),
                |  (3, DATE'1998-08-07', 1998),
                |  (10, DATE'1999-02-10', 1999),
                |  (11, DATE'1999-03-01', 1999)
                |AS t(d_date_sk, d_date, d_year)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW store AS
                |SELECT CAST(s_store_sk AS BIGINT) AS s_store_sk,
                |       CAST(s_store_id AS STRING) AS s_store_id
                |FROM VALUES
                |  (10, 'S10'),
                |  (20, 'S20')
                |AS t(s_store_sk, s_store_id)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW catalog_page AS
                |SELECT CAST(cp_catalog_page_sk AS BIGINT) AS cp_catalog_page_sk,
                |       CAST(cp_catalog_page_id AS STRING) AS cp_catalog_page_id
                |FROM VALUES
                |  (100, 'CP100'),
                |  (200, 'CP200')
                |AS t(cp_catalog_page_sk, cp_catalog_page_id)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW web_site AS
                |SELECT CAST(web_site_sk AS BIGINT) AS web_site_sk,
                |       CAST(web_site_id AS STRING) AS web_site_id
                |FROM VALUES
                |  (1000, 'W1000'),
                |  (2000, 'W2000')
                |AS t(web_site_sk, web_site_id)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW store_sales AS
                |SELECT CAST(ss_store_sk AS BIGINT) AS ss_store_sk,
                |       CAST(ss_sold_date_sk AS BIGINT) AS ss_sold_date_sk,
                |       CAST(ss_ext_sales_price AS DECIMAL(7,2)) AS ss_ext_sales_price,
                |       CAST(ss_net_profit AS DECIMAL(7,2)) AS ss_net_profit,
                |       CAST(ss_item_sk AS BIGINT) AS ss_item_sk,
                |       CAST(ss_cdemo_sk AS BIGINT) AS ss_cdemo_sk,
                |       CAST(ss_promo_sk AS BIGINT) AS ss_promo_sk,
                |       CAST(ss_quantity AS DECIMAL(7,2)) AS ss_quantity,
                |       CAST(ss_list_price AS DECIMAL(7,2)) AS ss_list_price,
                |       CAST(ss_coupon_amt AS DECIMAL(7,2)) AS ss_coupon_amt,
                |       CAST(ss_sales_price AS DECIMAL(7,2)) AS ss_sales_price
                |FROM VALUES
                |  (10, 1, 11.00, 3.00, 1001, 2001, 3001, 1.00, 10.00, 0.50, 9.50),
                |  (20, 2, 12.00, 4.00, 1002, 2002, 3002, 2.00, 20.00, 1.00, 19.00),
                |  (10, 3, 13.00, 5.00, 1001, 2001, 3001, 3.00, 30.00, 1.50, 28.50)
                |AS t(
                |  ss_store_sk, ss_sold_date_sk, ss_ext_sales_price, ss_net_profit,
                |  ss_item_sk, ss_cdemo_sk, ss_promo_sk,
                |  ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW item AS
                |SELECT CAST(i_item_sk AS BIGINT) AS i_item_sk,
                |       CAST(i_item_id AS STRING) AS i_item_id
                |FROM VALUES
                |  (1001, 'I1001'),
                |  (1002, 'I1002')
                |AS t(i_item_sk, i_item_id)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW promotion AS
                |SELECT CAST(p_promo_sk AS BIGINT) AS p_promo_sk,
                |       CAST(p_channel_email AS STRING) AS p_channel_email,
                |       CAST(p_channel_event AS STRING) AS p_channel_event
                |FROM VALUES
                |  (3001, 'N', 'Y'),
                |  (3002, 'Y', 'N')
                |AS t(p_promo_sk, p_channel_email, p_channel_event)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW customer_demographics AS
                |SELECT CAST(cd_demo_sk AS BIGINT) AS cd_demo_sk,
                |       CAST(cd_gender AS STRING) AS cd_gender,
                |       CAST(cd_marital_status AS STRING) AS cd_marital_status,
                |       CAST(cd_education_status AS STRING) AS cd_education_status
                |FROM VALUES
                |  (2001, 'F', 'W', 'Primary'),
                |  (2002, 'F', 'W', 'Primary')
                |AS t(cd_demo_sk, cd_gender, cd_marital_status, cd_education_status)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW store_returns AS
                |SELECT CAST(sr_store_sk AS BIGINT) AS sr_store_sk,
                |       CAST(sr_returned_date_sk AS BIGINT) AS sr_returned_date_sk,
                |       CAST(sr_return_amt AS DECIMAL(7,2)) AS sr_return_amt,
                |       CAST(sr_net_loss AS DECIMAL(7,2)) AS sr_net_loss
                |FROM VALUES
                |  (10, 1, 1.00, 0.50),
                |  (20, 2, 2.00, 0.25)
                |AS t(sr_store_sk, sr_returned_date_sk, sr_return_amt, sr_net_loss)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW catalog_sales AS
                |SELECT CAST(cs_catalog_page_sk AS BIGINT) AS cs_catalog_page_sk,
                |       CAST(cs_sold_date_sk AS BIGINT) AS cs_sold_date_sk,
                |       CAST(cs_ext_sales_price AS DECIMAL(7,2)) AS cs_ext_sales_price,
                |       CAST(cs_net_profit AS DECIMAL(7,2)) AS cs_net_profit,
                |       CAST(cs_order_number AS BIGINT) AS cs_order_number,
                |       CAST(cs_ext_ship_cost AS DECIMAL(7,2)) AS cs_ext_ship_cost,
                |       CAST(cs_ship_date_sk AS BIGINT) AS cs_ship_date_sk,
                |       CAST(cs_ship_addr_sk AS BIGINT) AS cs_ship_addr_sk,
                |       CAST(cs_call_center_sk AS BIGINT) AS cs_call_center_sk,
                |       CAST(cs_warehouse_sk AS BIGINT) AS cs_warehouse_sk
                |FROM VALUES
                |  (100, 1, 21.00, 5.00, 4001, 2.00, 1, 9001, 7001, 1),
                |  (200, 2, 22.00, 6.00, 4002, 3.00, 2, 9002, 7002, 1),
                |  (100, 10, 30.00, 2.00, 5001, 5.00, 10, 9001, 7001, 1),
                |  (200, 10, 35.00, 1.50, 5001, 3.00, 10, 9001, 7001, 2),
                |  (100, 10, 40.00, 1.00, 5002, 4.00, 10, 9001, 7001, 1)
                |AS t(
                |  cs_catalog_page_sk, cs_sold_date_sk, cs_ext_sales_price, cs_net_profit,
                |  cs_order_number, cs_ext_ship_cost, cs_ship_date_sk, cs_ship_addr_sk,
                |  cs_call_center_sk, cs_warehouse_sk)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW catalog_returns AS
                |SELECT CAST(cr_catalog_page_sk AS BIGINT) AS cr_catalog_page_sk,
                |       CAST(cr_returned_date_sk AS BIGINT) AS cr_returned_date_sk,
                |       CAST(cr_return_amount AS DECIMAL(7,2)) AS cr_return_amount,
                |       CAST(cr_net_loss AS DECIMAL(7,2)) AS cr_net_loss,
                |       CAST(cr_order_number AS BIGINT) AS cr_order_number
                |FROM VALUES
                |  (100, 1, 1.50, 0.30, 3001),
                |  (200, 2, 0.50, 0.10, 3002),
                |  (100, 10, 1.00, 0.10, 5002)
                |AS t(
                |  cr_catalog_page_sk, cr_returned_date_sk, cr_return_amount, cr_net_loss,
                |  cr_order_number)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW customer_address AS
                |SELECT CAST(ca_address_sk AS BIGINT) AS ca_address_sk,
                |       CAST(ca_state AS STRING) AS ca_state
                |FROM VALUES
                |  (9001, 'IL'),
                |  (9002, 'CA')
                |AS t(ca_address_sk, ca_state)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW call_center AS
                |SELECT CAST(cc_call_center_sk AS BIGINT) AS cc_call_center_sk,
                |       CAST(cc_county AS STRING) AS cc_county
                |FROM VALUES
                |  (7001, 'Williamson County'),
                |  (7002, 'Other County')
                |AS t(cc_call_center_sk, cc_county)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW web_sales AS
                |SELECT CAST(ws_web_site_sk AS BIGINT) AS ws_web_site_sk,
                |       CAST(ws_sold_date_sk AS BIGINT) AS ws_sold_date_sk,
                |       CAST(ws_ext_sales_price AS DECIMAL(7,2)) AS ws_ext_sales_price,
                |       CAST(ws_net_profit AS DECIMAL(7,2)) AS ws_net_profit,
                |       CAST(ws_item_sk AS BIGINT) AS ws_item_sk,
                |       CAST(ws_order_number AS BIGINT) AS ws_order_number
                |FROM VALUES
                |  (1000, 1, 31.00, 7.00, 500, 9000),
                |  (2000, 2, 32.00, 8.00, 600, 9001)
                |AS t(
                |  ws_web_site_sk, ws_sold_date_sk, ws_ext_sales_price,
                |  ws_net_profit, ws_item_sk, ws_order_number)
                |""".stripMargin)

    spark.sql("""
                |CREATE OR REPLACE TEMP VIEW web_returns AS
                |SELECT CAST(wr_returned_date_sk AS BIGINT) AS wr_returned_date_sk,
                |       CAST(wr_return_amt AS DECIMAL(7,2)) AS wr_return_amt,
                |       CAST(wr_net_loss AS DECIMAL(7,2)) AS wr_net_loss,
                |       CAST(wr_item_sk AS BIGINT) AS wr_item_sk,
                |       CAST(wr_order_number AS BIGINT) AS wr_order_number
                |FROM VALUES
                |  (1, 2.00, 0.60, 500, 9000),
                |  (2, 3.00, 0.40, 600, 9001)
                |AS t(wr_returned_date_sk, wr_return_amt, wr_net_loss, wr_item_sk, wr_order_number)
                |""".stripMargin)
  }

  private def optimizedAggregateCount(df: DataFrame): Int = {
    collect(df.queryExecution.executedPlan) { case _: HashAggregateExecBaseTransformer => 1 }.size
  }

  private def assertOptimizedAggregateCount(df: DataFrame, expected: Int): Unit = {
    val actual = optimizedAggregateCount(df)
    assert(
      actual == expected,
      s"Expected $expected Aggregate nodes in optimized plan, but got $actual.\n" +
        s"Optimized plan:\n${df.queryExecution.optimizedPlan.treeString}")
  }

  test("Join-aggregate wrapper aggregate") {
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

  test("Join-aggregate wrapper aggregate 2") {
    val query =
      """
        |SELECT
        |  l_discount, p_partkey, AVG(l_extendedprice)
        |FROM lineitem
        |JOIN part
        |  ON l_partkey = p_partkey
        |GROUP BY l_discount, p_partkey
        |ORDER BY l_discount, p_partkey
        |LIMIT 100
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
        checkGlutenPlan[HashAggregateExecTransformer](df)
    }
  }


  test("Support join-aggregate wrapper aggregate for simplified TPC-H q12") {
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

  test("Support join-aggregate wrapper aggregate for multi-field agg buffer (avg)") {
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

  test("Support join-aggregate wrapper aggregate with duplicate decimal sum buffers") {
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
            assertOptimizedAggregateCount(df, 2)
            checkGlutenPlan[HashAggregateExecTransformer](df)
        }
      }
    }
  }

  test("Minimal pushdown for decimal avg") {
    withTempView("ss_fact_avg", "ss_dim_avg") {
      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_fact_avg AS
                  |SELECT
                  |  CAST(item_sk AS INT) AS item_sk,
                  |  CAST(grp_id AS INT) AS grp_id,
                  |  CAST(metric AS DECIMAL(7, 2)) AS metric
                  |FROM VALUES
                  |  (1, 100, 10.00),
                  |  (1, 100, 20.00),
                  |  (2, 200, 30.00)
                  |AS t(item_sk, grp_id, metric)
                  |""".stripMargin)

      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_dim_avg AS
                  |SELECT CAST(item_sk AS INT) AS item_sk
                  |FROM VALUES
                  |  (1), (1), (2), (2), (2)
                  |AS t(item_sk)
                  |""".stripMargin)

      val query =
        """
          |SELECT
          |  f.grp_id,
          |  AVG(f.metric) AS avg_metric
          |FROM ss_fact_avg f
          |JOIN ss_dim_avg d
          |  ON f.item_sk = d.item_sk
          |GROUP BY f.grp_id
          |ORDER BY f.grp_id
          |""".stripMargin

      withSQLConf(
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.shuffle.partitions" -> "100",
        "spark.sql.autoBroadcastJoinThreshold" -> "10m") {
        runQueryAndCompare(query) {
          df =>
            assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
            assertOptimizedAggregateCount(df, 2)
            checkGlutenPlan[HashAggregateExecTransformer](df)
        }
      }
    }
  }

  test("Support full TPC-DS q5 shape") {
    val query =
      """
        |with ssr as
        | (select s_store_id,
        |        sum(sales_price) as sales,
        |        sum(profit) as profit,
        |        sum(return_amt) as returns,
        |        sum(net_loss) as profit_loss
        | from
        |  ( select  ss_store_sk as store_sk,
        |            ss_sold_date_sk  as date_sk,
        |            ss_ext_sales_price as sales_price,
        |            ss_net_profit as profit,
        |            cast(0 as decimal(7,2)) as return_amt,
        |            cast(0 as decimal(7,2)) as net_loss
        |    from store_sales
        |    union all
        |    select sr_store_sk as store_sk,
        |           sr_returned_date_sk as date_sk,
        |           cast(0 as decimal(7,2)) as sales_price,
        |           cast(0 as decimal(7,2)) as profit,
        |           sr_return_amt as return_amt,
        |           sr_net_loss as net_loss
        |    from store_returns
        |   ) salesreturns,
        |     date_dim,
        |     store
        | where date_sk = d_date_sk
        |       and d_date between cast('1998-08-04' as date)
        |                  and (cast('1998-08-04' as date) + interval '14' day)
        |       and store_sk = s_store_sk
        | group by s_store_id)
        | ,
        | csr as
        | (select cp_catalog_page_id,
        |        sum(sales_price) as sales,
        |        sum(profit) as profit,
        |        sum(return_amt) as returns,
        |        sum(net_loss) as profit_loss
        | from
        |  ( select  cs_catalog_page_sk as page_sk,
        |            cs_sold_date_sk  as date_sk,
        |            cs_ext_sales_price as sales_price,
        |            cs_net_profit as profit,
        |            cast(0 as decimal(7,2)) as return_amt,
        |            cast(0 as decimal(7,2)) as net_loss
        |    from catalog_sales
        |    union all
        |    select cr_catalog_page_sk as page_sk,
        |           cr_returned_date_sk as date_sk,
        |           cast(0 as decimal(7,2)) as sales_price,
        |           cast(0 as decimal(7,2)) as profit,
        |           cr_return_amount as return_amt,
        |           cr_net_loss as net_loss
        |    from catalog_returns
        |   ) salesreturns,
        |     date_dim,
        |     catalog_page
        | where date_sk = d_date_sk
        |       and d_date between cast('1998-08-04' as date)
        |                  and (cast('1998-08-04' as date) + interval '14' day)
        |       and page_sk = cp_catalog_page_sk
        | group by cp_catalog_page_id)
        | ,
        | wsr as
        | (select web_site_id,
        |        sum(sales_price) as sales,
        |        sum(profit) as profit,
        |        sum(return_amt) as returns,
        |        sum(net_loss) as profit_loss
        | from
        |  ( select  ws_web_site_sk as wsr_web_site_sk,
        |            ws_sold_date_sk  as date_sk,
        |            ws_ext_sales_price as sales_price,
        |            ws_net_profit as profit,
        |            cast(0 as decimal(7,2)) as return_amt,
        |            cast(0 as decimal(7,2)) as net_loss
        |    from web_sales
        |    union all
        |    select ws_web_site_sk as wsr_web_site_sk,
        |           wr_returned_date_sk as date_sk,
        |           cast(0 as decimal(7,2)) as sales_price,
        |           cast(0 as decimal(7,2)) as profit,
        |           wr_return_amt as return_amt,
        |           wr_net_loss as net_loss
        |    from web_returns left outer join web_sales on
        |         ( wr_item_sk = ws_item_sk
        |           and wr_order_number = ws_order_number)
        |   ) salesreturns,
        |     date_dim,
        |     web_site
        | where date_sk = d_date_sk
        |       and d_date between cast('1998-08-04' as date)
        |                  and (cast('1998-08-04' as date) + interval '14' day)
        |       and wsr_web_site_sk = web_site_sk
        | group by web_site_id)
        |  select  channel
        |        , id
        |        , sum(sales) as sales
        |        , sum(returns) as returns
        |        , sum(profit) as profit
        | from
        | (select 'store channel' as channel
        |        , 'store' || s_store_id as id
        |        , sales
        |        , returns
        |        , (profit - profit_loss) as profit
        | from   ssr
        | union all
        | select 'catalog channel' as channel
        |        , 'catalog_page' || cp_catalog_page_id as id
        |        , sales
        |        , returns
        |        , (profit - profit_loss) as profit
        | from  csr
        | union all
        | select 'web channel' as channel
        |        , 'web_site' || web_site_id as id
        |        , sales
        |        , returns
        |        , (profit - profit_loss) as profit
        | from   wsr
        | ) x
        | group by rollup (channel, id)
        | order by channel
        |         ,id
        | LIMIT 100
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
        checkGlutenPlan[HashAggregateExecTransformer](df)
    }
  }

  test("Support simplified TPC-DS q7 shape") {
    val query =
      """
        |select  i_item_id,
        |        avg(ss_quantity) agg1
        |from store_sales, item
        |where ss_item_sk = i_item_sk
        |group by i_item_id
        |order by i_item_id
        |limit 100
        |""".stripMargin

    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.shuffle.partitions" -> "100",
      "spark.sql.autoBroadcastJoinThreshold" -> "10m") {
      runQueryAndCompare(query) {
        df =>
          assert(df.queryExecution.optimizedPlan.toString().contains("ss_agg_wrapper_"))
          checkGlutenPlan[HashAggregateExecTransformer](df)
      }
    }
  }

  test("Support simplified TPC-DS q16 shape") {
    val query =
      """
        |select
        |   count(distinct cs_order_number) as `order count`
        |  ,sum(cs_ext_ship_cost) as `total shipping cost`
        |  ,sum(cs_net_profit) as `total net profit`
        |from
        |   catalog_sales cs1
        |  ,date_dim
        |  ,customer_address
        |  ,call_center
        |where
        |    d_date between '1999-2-01' and
        |           (cast('1999-2-01' as date) + interval '60' day)
        |and cs1.cs_ship_date_sk = d_date_sk
        |and cs1.cs_ship_addr_sk = ca_address_sk
        |and ca_state = 'IL'
        |and cs1.cs_call_center_sk = cc_call_center_sk
        |and cc_county in ('Williamson County','Williamson County','Williamson County',
        |                  'Williamson County','Williamson County')
        |and exists (select *
        |            from catalog_sales cs2
        |            where cs1.cs_order_number = cs2.cs_order_number
        |              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
        |and not exists(select *
        |               from catalog_returns cr1
        |               where cs1.cs_order_number = cr1.cr_order_number)
        |order by count(distinct cs_order_number)
        |limit 100
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        // Mixed distinct + non-distinct aggregate shape is currently not pushed by the
        // Join-aggregate pre-aggregate rule.
        checkGlutenPlan[HashAggregateExecTransformer](df)
    }
  }

  test("Support simplified SUM + COUNT DISTINCT shape") {
    withTempView("ss_fact_distinct", "ss_dim_distinct") {
      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_fact_distinct AS
                  |SELECT
                  |  CAST(item_sk AS INT) AS item_sk,
                  |  CAST(grp_id AS INT) AS grp_id,
                  |  CAST(metric AS DECIMAL(7, 2)) AS metric,
                  |  CAST(order_no AS BIGINT) AS order_no
                  |FROM VALUES
                  |  (1, 100, 10.00, 9001),
                  |  (1, 100, 20.00, 9002),
                  |  (2, 200, 30.00, 9002),
                  |  (2, 200, 15.00, 9003)
                  |AS t(item_sk, grp_id, metric, order_no)
                  |""".stripMargin)

      spark.sql("""
                  |CREATE OR REPLACE TEMP VIEW ss_dim_distinct AS
                  |SELECT CAST(item_sk AS INT) AS item_sk
                  |FROM VALUES
                  |  (1), (2)
                  |AS t(item_sk)
                  |""".stripMargin)

      val query =
        """
          |SELECT
          |  f.grp_id,
          |  SUM(f.metric) AS sum_metric,
          |  COUNT(DISTINCT f.order_no) AS distinct_orders
          |FROM ss_fact_distinct f
          |JOIN ss_dim_distinct d
          |  ON f.item_sk = d.item_sk
          |GROUP BY f.grp_id
          |ORDER BY f.grp_id
          |""".stripMargin

      runQueryAndCompare(query) {
        df =>
          checkGlutenPlan[HashAggregateExecTransformer](df)
      }
    }
  }

  test("Support join-aggregate wrapper aggregate for simplified TPC-H q18") {
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
