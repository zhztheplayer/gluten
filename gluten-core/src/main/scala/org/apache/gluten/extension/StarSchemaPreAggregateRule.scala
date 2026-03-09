package org.apache.gluten.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class StarSchemaPreAggregateRule(spark: SparkSession) extends Rule[LogicalPlan] {
  ???
}
