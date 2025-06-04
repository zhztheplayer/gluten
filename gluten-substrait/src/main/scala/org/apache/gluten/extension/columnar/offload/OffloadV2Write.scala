package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.connector.write.ColumnarWrite
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec

class OffloadV2Write(writeFunc: Write => Option[ColumnarWrite]) extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case append: AppendDataExec =>
      writeFunc(append.write)
        .map {
          cw =>
            val newAppend = append.copy(write = cw)
            newAppend.copyTagsFrom(append)
            newAppend
        }
        .getOrElse(append)
      ??? // TODO
  }
}
