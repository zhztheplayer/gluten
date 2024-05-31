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
package org.apache.gluten.extension.columnar

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.extension.columnar.transition.Convention.KnownBatchType
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{DataSourceScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.vectorized.ColumnarBatch

case class PartitioningDerivation(child: SparkPlan) extends UnaryExecNode with KnownBatchType {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(newChild)
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = child.output
  override val batchType: Convention.BatchType = Convention.get(child).batchType
  override def supportsColumnar: Boolean = child.supportsColumnar
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override lazy val outputPartitioning: Partitioning = {
    if (child.outputPartitioning.numPartitions != 0) {
      child.outputPartitioning
    } else {
      val rdd = if (SparkShimLoader.getSparkShims.supportsRowBased(child)) {
        child.execute()
      } else {
        child.executeColumnar()
      }
      val num = rdd.getNumPartitions
      UnknownPartitioning(num)
    }
  }
}

object PartitioningDerivation {
  object Add extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!GlutenConfig.getConf.partitioningDerivation) {
        return plan
      }
      plan.transformUp {
        case d: DataSourceScanExec =>
          PartitioningDerivation(d)
        case d: DataSourceV2ScanExecBase =>
          PartitioningDerivation(d)
      }
    }
  }

  object Remove extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
      case PartitioningDerivation(c) => c
    }
  }
}
