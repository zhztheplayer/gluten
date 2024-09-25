package org.apache.gluten.columnarbatch

import org.apache.gluten.extension.columnar.transition.Convention.BatchType.VanillaBatch
import org.apache.gluten.extension.columnar.transition.{Convention, TransitionDef}
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}

object ArrowBatches {

  /**
   * NativeArrowBatch stands for Gluten's native Arrow-based columnar batch implementation.
   *
   * NativeArrowBatch should have [[org.apache.gluten.columnarbatch.IndicatorVector]] set as the first
   * vector. NativeArrowBatch can be loaded to JavaArrowBatch through API in [[ColumnarBatches]].
   */
  object NativeArrowBatch extends Convention.BatchType {
    toRow(
      () =>
        (plan: SparkPlan) => {
          ColumnarToRowExec(ColumnarToColumnarExec(plan))
        })

    // Arrow batch is one-way compatible with vanilla batch since it provides valid
    // #get<type>(...) implementations.
    toBatch(VanillaBatch, TransitionDef.empty)
  }

  /**
   * JavaArrowBatch stands for Gluten's Java Arrow-based columnar batch implementation.
   *
   * JavaArrowBatch should have [[org.apache.gluten.vectorized.ArrowWritableColumnVector]]s populated
   * in it. JavaArrowBatch can be offloaded to NativeArrowBatch through API in [[ColumnarBatches]].
   */
  object JavaArrowBatch extends Convention.BatchType {

  }

  private case class LoadArrowDataExec(child: SparkPlan)
}
