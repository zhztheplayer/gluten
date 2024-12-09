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
import org.apache.gluten.backendsapi.velox.VeloxBackend
import org.apache.gluten.execution.IcebergScanTransformer
import org.apache.gluten.extension.columnar.enumerated.RasOffload
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class VeloxIcebergComponent extends Component {
  import VeloxIcebergComponent._
  override def name(): String = "velox-iceberg"
  override def buildInfo(): Component.BuildInfo =
    Component.BuildInfo("VeloxIceberg", "N/A", "N/A", "N/A")
  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil
  override def injectRules(injector: Injector): Unit = {
    // Inject legacy rule.
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(OffloadIcebergScan())
        HeuristicTransform.Simple(
          Validators.newValidator(c.glutenConf, offload),
          offload
        )
    }

    // Inject RAS rule.
    injector.gluten.ras.injectRasRule {
      c =>
        RasOffload.Rule(
          RasOffload.from[BatchScanExec](OffloadIcebergScan()),
          Validators.newValidator(c.glutenConf),
          Nil)
    }
  }
}

object VeloxIcebergComponent {
  private case class OffloadIcebergScan() extends OffloadSingleNode {
    override def offload(plan: SparkPlan): SparkPlan = plan match {
      case scan: BatchScanExec if IcebergScanTransformer.supportsBatchScan(scan.scan) =>
        IcebergScanTransformer(scan)
      case other => other
    }
  }
}