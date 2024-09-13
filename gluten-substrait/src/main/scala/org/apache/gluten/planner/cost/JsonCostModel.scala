package org.apache.gluten.planner.cost
import org.apache.spark.sql.execution.SparkPlan

class JsonCostModel(path: String) extends LongCostModel {
  override def selfLongCostOf(node: SparkPlan): Long = ???
}
