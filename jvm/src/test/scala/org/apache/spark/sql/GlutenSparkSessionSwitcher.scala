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

package org.apache.spark.sql

import org.apache.spark.internal.config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.{DebugFilesystem, SparkConf}

class GlutenSparkSessionSwitcher(testConf: SparkConf, baselineConf: SparkConf) {

  private val testDefaults = new SparkConf()
    .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
    .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
    .set(SQLConf.CODEGEN_FALLBACK.key, "false")
    .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
    // Disable ConvertToLocalRelation for better test coverage. Test cases built on
    // LocalRelation will exercise the optimization rules better by disabling it as
    // this rule may potentially block testing of other optimization rules such as
    // ConstantPropagation etc.
    .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)

  testDefaults.set(
    StaticSQLConf.WAREHOUSE_PATH,
    testDefaults.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)

  private var _spark: TestSparkSession = null
  private var _sessionType: Int = GlutenSparkSessionSwitcher.SESSION_TYPE_NONE

  def useTestSession(): Unit = synchronized {
    if (_sessionType == GlutenSparkSessionSwitcher.SESSION_TYPE_TEST) {
      return
    }
    // scalastyle:off println
    println("Switching to test session... ")
    // scalastyle:on println
    stopActiveSession()
    val conf = new SparkConf()
      .setAll(testDefaults.getAll)
      .setAll(testConf.getAll)
    activateSession(conf)
    _sessionType = GlutenSparkSessionSwitcher.SESSION_TYPE_TEST
    // scalastyle:off println
    println("Successfully switched to test session. ")
    // scalastyle:on println
  }

  def useBaselineSession(): Unit = synchronized {
    if (_sessionType == GlutenSparkSessionSwitcher.SESSION_TYPE_BASELINE) {
      return
    }
    // scalastyle:off println
    println("Switching to baseline session... ")
    // scalastyle:on println
    stopActiveSession()
    val conf = new SparkConf()
      .setAll(testDefaults.getAll)
      .setAll(baselineConf.getAll)
    activateSession(conf)
    _sessionType = GlutenSparkSessionSwitcher.SESSION_TYPE_BASELINE
    // scalastyle:off println
    println("Successfully switched to baseline session. ")
    // scalastyle:on println
  }

  def stopActiveSession(): Unit = synchronized {
    try {
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
          _sessionType = GlutenSparkSessionSwitcher.SESSION_TYPE_NONE
        }
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  def spark(): SparkSession = {
    _spark
  }

  private def hasActiveSession(): Boolean = {
    _spark != null
  }

  private def createSession(conf: SparkConf): Unit = {
    if (hasActiveSession()) {
      throw new IllegalStateException()
    }
    _spark = new TestSparkSession(conf)
  }

  private def activateSession(conf: SparkConf): Unit = {
    SparkSession.cleanupAnyExistingSession()
    if (hasActiveSession()) {
      stopActiveSession()
    }
    createSession(conf)
  }
}

object GlutenSparkSessionSwitcher {
  private val SESSION_TYPE_NONE = -1
  private val SESSION_TYPE_TEST = 0
  private val SESSION_TYPE_BASELINE = 1
}
