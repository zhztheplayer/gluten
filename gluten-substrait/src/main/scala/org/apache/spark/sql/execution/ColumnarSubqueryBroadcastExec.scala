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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.metrics.GlutenTimeMetric

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelation, HashJoin, LongHashedRelation}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.ThreadUtils

import scala.concurrent.Future
import scala.concurrent.duration.Duration

case class ColumnarSubqueryBroadcastExec(
    name: String,
    index: Int,
    buildKeys: Seq[Expression],
    child: SparkPlan)
  extends BaseSubqueryExec
  with UnaryExecNode
  with GlutenPlan {

  // `ColumnarSubqueryBroadcastExec` is only used with `InSubqueryExec`.
  // No one would reference this output,
  // so the exprId doesn't matter here. But it's important to correctly report the output length, so
  // that `InSubqueryExec` can know it's the single-column execution mode, not multi-column.
  override def output: Seq[Attribute] = {
    val key = buildKeys(index)
    val name = key match {
      case n: NamedExpression => n.name
      case Cast(n: NamedExpression, _, _, _) => n.name
      case _ => "key"
    }
    Seq(AttributeReference(name, key.dataType, key.nullable)())
  }

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genColumnarSubqueryBroadcastMetrics(sparkContext)

  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    copy(name = "native-dpp", buildKeys = keys, child = child.canonicalized)
  }

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(session, executionId) {
        val rows = GlutenTimeMetric.millis(longMetric("collectTime")) {
          _ =>
            val relation = child.executeBroadcast[Any]().value
            relation match {
              case b: BuildSideRelation =>
                // Transform columnar broadcast value to Array[InternalRow] by key.
                b.transform(buildKeys(index)).distinct
              case h: HashedRelation =>
                val (iter, expr) = if (h.isInstanceOf[LongHashedRelation]) {
                  (h.keys(), HashJoin.extractKeyExprAt(buildKeys, index))
                } else {
                  (
                    h.keys(),
                    BoundReference(index, buildKeys(index).dataType, buildKeys(index).nullable))
                }
                val proj = UnsafeProjection.create(expr)
                val keyIter = iter.map(proj).map(_.copy())
                keyIter.toArray[InternalRow].distinct
              case other =>
                throw new UnsupportedOperationException(
                  s"Unrecognizable broadcast relation: $other")
            }
        }
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes).sum
        longMetric("dataSize") += dataSize
        longMetric("numOutputRows") += rows.length
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        rows
      }
    }(SubqueryBroadcastExec.executionContext)
  }

  override protected def doPrepare(): Unit = {
    relationFuture
  }

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "SubqueryBroadcastExec does not support the execute() code path.")
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")

  protected def withNewChildInternal(newChild: SparkPlan): ColumnarSubqueryBroadcastExec =
    copy(child = newChild)
}