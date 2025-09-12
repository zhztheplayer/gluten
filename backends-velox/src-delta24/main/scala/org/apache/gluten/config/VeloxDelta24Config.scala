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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

class VeloxDelta24Config(conf: SQLConf) extends GlutenCoreConfig(conf) {
  import VeloxDelta24Config._

  def deltaNativeDvEnabled(): Boolean = {
    getConf(DELTA_NATIVE_DV_ENABLED)
  }
}

object VeloxDelta24Config {
  def get: VeloxDelta24Config = {
    new VeloxDelta24Config(SQLConf.get)
  }

  val DELTA_NATIVE_DV_ENABLED = GlutenCoreConfig
    .buildConf("spark.gluten.delta.nativeDv.enabled")
    .doc("Enables native deletion vectors for Delta Lake scan")
    .booleanConf
    .createWithDefault(true)
}
