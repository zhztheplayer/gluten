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

package io.glutenproject.e2e.tpc.h

import io.glutenproject.e2e.tpc.TpcRunner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{GlutenSparkSessionSwitcher, GlutenTestUtils, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkFunSuite}

import scala.collection.JavaConverters._

abstract class TpchSuite extends SparkFunSuite {
  private var sessionSwitcher: GlutenSparkSessionSwitcher = null
  private var runner: TpcRunner = null

  private val typeModifiers: java.util.List[TypeModifier] = new java.util.ArrayList[TypeModifier]()

  protected def defineTypeModifier(typeModifier: TypeModifier): Unit = {
    typeModifiers.add(typeModifier)
  }

  def testConf(): SparkConf

  def queryResource(): String

  override def beforeAll(): Unit = {
    super.beforeAll()
    runner = new TpcRunner(queryResource(), TpchSuite.TPCH_WRITE_PATH)
    LogManager.getRootLogger.setLevel(Level.WARN)
    sessionSwitcher = new GlutenSparkSessionSwitcher(
      testConf(), TpchSuite.baselineConf)
    sessionSwitcher.useBaselineSession() // use vanilla spark to generate data
    val dataGen = new TpchDataGen(sessionSwitcher.spark(), 0.1D, TpchSuite.TPCH_WRITE_PATH,
      typeModifiers.asScala.toArray)
    dataGen.gen()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sessionSwitcher.stopActiveSession()
  }

  test("q1") {
    runTpchQuery("q1")
  }

  test("q2") {
    runTpchQuery("q2")
  }

  test("q3") {
    runTpchQuery("q3")
  }

  test("q4") {
    runTpchQuery("q4")
  }

  test("q5") {
    runTpchQuery("q5")
  }

  test("q6") {
    runTpchQuery("q6")
  }

  test("q7") {
    runTpchQuery("q7")
  }

  test("q8") {
    runTpchQuery("q8")
  }

  test("q9") {
    runTpchQuery("q9")
  }

  test("q10") {
    runTpchQuery("q10")
  }

  test("q11") {
    runTpchQuery("q11")
  }

  test("q12") {
    runTpchQuery("q12")
  }

  test("q13") {
    runTpchQuery("q13")
  }

  test("q14") {
    runTpchQuery("q14")
  }

  test("q15") {
    runTpchQuery("q15")
  }

  test("q16") {
    runTpchQuery("q16")
  }

  test("q17") {
    runTpchQuery("q17")
  }

  test("q18") {
    runTpchQuery("q18")
  }

  test("q19") {
    runTpchQuery("q19")
  }

  test("q20") {
    runTpchQuery("q20")
  }

  test("q21") {
    runTpchQuery("q21")
  }

  test("q22") {
    runTpchQuery("q22")
  }

  private def runTpchQuery(id: String): Unit = {
    // scalastyle:off println
    println(s"Running query: $id... ")
    // scalastyle:on println
    sessionSwitcher.useTestSession()
    runner.createTables(sessionSwitcher.spark())
    val result = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = true)
    sessionSwitcher.useBaselineSession()
    runner.createTables(sessionSwitcher.spark())
    val expected = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = true)
    val error = GlutenTestUtils.compareAnswers(result, expected, sort = true)
    if (error.isEmpty) {
      return
    }
    fail(error.get)
  }

  private def runTextQuery(query: String): Unit = {
    sessionSwitcher.useTestSession()
    runner.createTables(sessionSwitcher.spark())
    val result = TpchSuite.runTextQuery(sessionSwitcher.spark(), query, explain = true)
    sessionSwitcher.useBaselineSession()
    runner.createTables(sessionSwitcher.spark())
    val expected = TpchSuite.runTextQuery(sessionSwitcher.spark(), query, explain = true)
    val error = GlutenTestUtils.compareAnswers(result, expected, sort = true)
    if (error.isEmpty) {
      return
    }
    fail(error.get)
  }
}

object TpchSuite {
  private val MAX_DIRECT_MEMORY = "6g"
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"

  private val baselineConf = new SparkConf()
    .set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
    .set("spark.sql.codegen.wholeStage", "false")
    .set("spark.sql.sources.useV1SourceList", "")
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
    .set("spark.executor.heartbeatInterval", "3600000")
    .set("spark.network.timeout", "3601s")
    .set("spark.unsafe.exceptionOnMemoryLeak", "false")
    .set("spark.network.io.preferDirectBufs", "false")

  private def runTextQuery(spark: SparkSession, sql: String, explain: Boolean = false): Seq[Row] = {
    val df = spark.sql(sql)
    if (explain) {
      df.explain(extended = true)
    }
    df.collect()
  }
}
