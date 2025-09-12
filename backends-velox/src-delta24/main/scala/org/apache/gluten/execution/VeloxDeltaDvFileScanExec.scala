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

import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.delta.{DeltaParquetFileFormat, VeloxDeltaParquetFileFormat}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.{FileSourceScanExec, FileSourceScanLike}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

case class VeloxDeltaDvFileScanExec private (delegateScanExec: FileSourceScanExec) extends FileSourceScanLike with GlutenPlan {

  override def dataFilters: Seq[Expression] = delegateScanExec.dataFilters

  override def disableBucketedScan: Boolean = delegateScanExec.disableBucketedScan

  override def optionalBucketSet: Option[BitSet] = delegateScanExec.optionalBucketSet

  override def optionalNumCoalescedBuckets: Option[Int] = delegateScanExec.optionalNumCoalescedBuckets

  override def output: Seq[Attribute] = delegateScanExec.output

  override def partitionFilters: Seq[Expression] = delegateScanExec.partitionFilters

  override def relation: HadoopFsRelation = delegateScanExec.relation

  override def requiredSchema: StructType = delegateScanExec.requiredSchema

  override def tableIdentifier: Option[TableIdentifier] = delegateScanExec.tableIdentifier

  override def batchType(): Convention.BatchType = {
    if (delegateScanExec.supportsColumnar) {
      Convention.BatchType.VanillaBatchType
    } else {
      Convention.BatchType.None
    }
  }

  override def rowType0(): Convention.RowType = {
    if (delegateScanExec.supportsColumnar) {
      Convention.RowType.None
    } else {
      Convention.RowType.VanillaRowType
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = delegateScanExec.inputRDDs()

  override protected def doExecute(): RDD[InternalRow] = delegateScanExec.execute()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = delegateScanExec.executeColumnar()
}

object VeloxDeltaDvFileScanExec {
  def apply(sparkFileScanExec: FileSourceScanExec): VeloxDeltaDvFileScanExec = {
    val delegateScanExec = {
      FileSourceScanExec(
        HadoopFsRelation(
          sparkFileScanExec.relation.location,
          sparkFileScanExec.relation.partitionSchema,
          sparkFileScanExec.relation.dataSchema,
          sparkFileScanExec.relation.bucketSpec,
          VeloxDeltaParquetFileFormat(sparkFileScanExec.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]),
          sparkFileScanExec.relation.options
        )(sparkFileScanExec.relation.sparkSession),
        sparkFileScanExec.output,
        sparkFileScanExec.requiredSchema,
        sparkFileScanExec.partitionFilters,
        sparkFileScanExec.optionalBucketSet,
        sparkFileScanExec.optionalNumCoalescedBuckets,
        sparkFileScanExec.dataFilters,
        sparkFileScanExec.tableIdentifier,
        sparkFileScanExec.disableBucketedScan
      )
    }
    new VeloxDeltaDvFileScanExec(delegateScanExec)
  }
}
