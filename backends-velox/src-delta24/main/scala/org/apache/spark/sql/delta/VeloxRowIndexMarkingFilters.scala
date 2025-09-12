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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.expr.{NativeExpressionEvaluator, NativeExprSet}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.substrait.`type`.{BooleanTypeNode, TypeBuilder, TypeNode}
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{DropAllRowsFilter, KeepAllRowsFilter, RoaringBitmapArray, RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, VectorSchemaRoot}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._

object VeloxRowIndexMarkingFilters {
  object VeloxDropMarkedRowsFilter extends VeloxRowIndexMarkingFiltersBuilder {
    override def getFilterForEmptyDeletionVector(): RowIndexFilter = KeepAllRowsFilter
    override def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RowIndexFilter = {
      new VeloxDropMarkedRowsFilter(bitmap)
    }
  }

  object VeloxKeepMarkedRowsFilter extends VeloxRowIndexMarkingFiltersBuilder {
    override def getFilterForEmptyDeletionVector(): RowIndexFilter = DropAllRowsFilter
    override def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RowIndexFilter = {
      new VeloxKeepMarkedRowsFilter(bitmap)
    }
  }

  sealed abstract class VeloxRowIndexMarkingFilters(bitmap: RoaringBitmapArray)
    extends RowIndexFilter {
    val valueWhenContained: Byte
    val valueWhenNotContained: Byte

    private val arrowAlloc: BufferAllocator = ArrowBufferAllocators.contextInstance()

    private val roaringBitmapArrayEvaluator: NativeExpressionEvaluator =
      new NativeExpressionEvaluator(
        Runtimes.contextInstance(BackendsApiManager.getBackendName, "VeloxRowIndexMarkingFilters"))

    private val roaringBitmapContainsExprSet: NativeExprSet = {
      val functionId = 0
      val functionName = "roaring_bitmap_array_contains"

      val childExpressions: Seq[ExpressionNode] = Seq(
        ExpressionBuilder.makeBinaryLiteral(
          bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)),
        ExpressionBuilder.makeSelection(1)
      )

      val bitmapContainsFunction = ExpressionBuilder.makeScalarFunction(
        functionId,
        childExpressions.asJava,
        new BooleanTypeNode(true))

      val childTypes = TypeBuilder.makeStruct(
        true,
        Seq[TypeNode](TypeBuilder.makeBinary(true), TypeBuilder.makeI64(true)).asJava,
        Seq[String]("bitmap", "value").asJava)

      roaringBitmapArrayEvaluator.createExprSet(
        bitmapContainsFunction.toProtobuf,
        childTypes.toProtobuf,
        Array(functionName))
    }

    override def materializeIntoVector(
        start: Long,
        end: Long,
        batch: WritableColumnVector): Unit = {
      val arrowRowIndexVector = new BigIntVector("value", arrowAlloc)
      val rowIdCount = (end - start).toInt
      arrowRowIndexVector.setValueCount(rowIdCount)
      for (rowId <- 0 until rowIdCount) {
        arrowRowIndexVector.set(rowId, start + rowId)
      }
      materializeIntoVectorWithArrowRowIndex(rowIdCount, arrowRowIndexVector, batch)
    }

    private def materializeIntoVectorWithArrowRowIndex(
        rowIdCount: Int,
        arrowRowIndexVector: BigIntVector,
        batch: WritableColumnVector): Unit = {
      assert(arrowRowIndexVector.getValueCount == rowIdCount)
      val arrowRowIndexVsr = new VectorSchemaRoot(Seq[FieldVector](arrowRowIndexVector).asJava)
      val vectors = ArrowWritableColumnVector
        .loadColumns(arrowRowIndexVsr.getRowCount, arrowRowIndexVsr.getFieldVectors)
        .toArray[ColumnVector]
      val columnarBatch = new ColumnarBatch(vectors, rowIdCount)
      val veloxBatch = VeloxColumnarBatches.ensureVeloxBatch(columnarBatch)
      val veloxOutBatch =
        roaringBitmapArrayEvaluator.evaluate(roaringBitmapContainsExprSet, veloxBatch)
      val arrowOutBatch = ColumnarBatches.load(arrowAlloc, veloxOutBatch)
      assert(arrowOutBatch.numCols() == 1)
      assert(arrowOutBatch.numRows() == rowIdCount)
      val arrowOutVector = arrowOutBatch.column(0)
      for (rowId <- 0 until rowIdCount) {
        val value = if (arrowOutVector.getBoolean(rowId)) {
          valueWhenContained
        } else {
          valueWhenNotContained
        }
        batch.putByte(rowId, value)
      }
      arrowOutBatch.close()
      veloxBatch.close()
    }
  }

  final class VeloxDropMarkedRowsFilter(bitmap: RoaringBitmapArray)
    extends VeloxRowIndexMarkingFilters(bitmap) {
    override val valueWhenContained: Byte = RowIndexFilter.DROP_ROW_VALUE
    override val valueWhenNotContained: Byte = RowIndexFilter.KEEP_ROW_VALUE
  }

  final class VeloxKeepMarkedRowsFilter(bitmap: RoaringBitmapArray)
    extends VeloxRowIndexMarkingFilters(bitmap) {
    override val valueWhenContained: Byte = RowIndexFilter.KEEP_ROW_VALUE
    override val valueWhenNotContained: Byte = RowIndexFilter.DROP_ROW_VALUE
  }

  trait VeloxRowIndexMarkingFiltersBuilder {
    def getFilterForEmptyDeletionVector(): RowIndexFilter
    def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RowIndexFilter

    def createInstance(
        deletionVector: DeletionVectorDescriptor,
        hadoopConf: Configuration,
        tablePath: Option[Path]): RowIndexFilter = {
      if (deletionVector.cardinality == 0) {
        getFilterForEmptyDeletionVector()
      } else {
        require(tablePath.nonEmpty, "Table path is required for non-empty deletion vectors")
        val dvStore = DeletionVectorStore.createInstance(hadoopConf)
        val storedBitmap = StoredBitmap.create(deletionVector, tablePath.get)
        val bitmap = storedBitmap.load(dvStore)
        getFilterForNonEmptyDeletionVector(bitmap)
      }
    }
  }
}
