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

package org.apache.gluten.component

import org.apache.gluten.columnarbatch.RapidsBatch
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionFunc, ConventionReq}
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.SparkReflectionUtil

import com.nvidia.spark.rapids.{GpuExec, GpuOverrides}

/**
 * Yet a component. Should become Backend and remove the dependency `VeloxBackend` when coming out
 * from PoC.
 */
class RapidsBackend extends Component {
  override def name(): String = "rapids"
  override def buildInfo(): Component.BuildInfo =
    Component.BuildInfo("Rapids", "N/A", "N/A", "N/A")
  override def dependencies(): Seq[Class[_ <: Component]] =
    SparkReflectionUtil.classForName("org.apache.gluten.backendsapi.velox.VeloxBackend") :: Nil
  override def injectRules(injector: Injector): Unit = {
    injector.gluten.legacy.injectTransform(_ => GpuOverrides())
  }

  override def convFuncOverride(): ConventionFunc.Override = {
    new ConventionFunc.Override {
      override def rowTypeOf: PartialFunction[SparkPlan, Convention.RowType] = {
        case _: GpuExec => Convention.RowType.None
      }
      override def batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = {
        case _: GpuExec => RapidsBatch
      }
      override def conventionReqOf: PartialFunction[SparkPlan, Seq[ConventionReq]] = {
        case p: GpuExec =>
          Seq.tabulate(p.children.size)(
            _ => ConventionReq.ofBatch(ConventionReq.BatchType.Is(RapidsBatch)))
      }
    }
  }
}
