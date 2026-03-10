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

class StarSchemaWrapperAggregateSuite extends VeloxTPCHTableSupport {
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
}
