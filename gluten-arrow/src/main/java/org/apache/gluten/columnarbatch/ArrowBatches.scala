package org.apache.gluten.columnarbatch

import org.apache.gluten.execution.ColumnarToColumnarExec
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.extension.columnar.transition.Convention.BatchType.VanillaBatch
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

object ArrowBatches {

  /**
   * NativeArrowBatch stands for Gluten's native Arrow-based columnar batch implementation.
   *
   * NativeArrowBatch should have [[org.apache.gluten.columnarbatch.IndicatorVector]] set as the
   * first vector. NativeArrowBatch can be loaded to JavaArrowBatch through API in
   * [[ColumnarBatches]].
   */
  object NativeArrowBatch extends Convention.BatchType {
    toRow(
      () =>
        (plan: SparkPlan) => {
          ColumnarToRowExec(LoadArrowDataExec(plan))
        })

    fromRow(
      () =>
        (plan: SparkPlan) => {
          OffloadArrowDataExec(RowToColumnarExec(plan))
        })

    toBatch(
      VanillaBatch,
      () =>
        (plan: SparkPlan) => {
          OffloadArrowDataExec(plan)
        })

    fromBatch(
      VanillaBatch,
      () =>
        (plan: SparkPlan) => {
          LoadArrowDataExec(plan)
        })
  }

  /**
   * JavaArrowBatch stands for Gluten's Java Arrow-based columnar batch implementation.
   *
   * JavaArrowBatch should have [[org.apache.gluten.vectorized.ArrowWritableColumnVector]]s
   * populated in it. JavaArrowBatch can be offloaded to NativeArrowBatch through API in
   * [[ColumnarBatches]].
   *
   * JavaArrowBatch is compatible with vanilla batch since it provides valid #get<type>(...)
   * implementations.
   */
  object JavaArrowBatch extends Convention.BatchType {}

  private case class LoadArrowDataExec(override val child: SparkPlan)
    extends ColumnarToColumnarExec(NativeArrowBatch, JavaArrowBatch) {
    override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
      in.map(b => ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance, b))
    }
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = child)
  }

  private case class OffloadArrowDataExec(override val child: SparkPlan)
    extends ColumnarToColumnarExec(JavaArrowBatch, NativeArrowBatch) {
    override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
      in.map(b => ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance, b))
    }
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = child)
  }
}
