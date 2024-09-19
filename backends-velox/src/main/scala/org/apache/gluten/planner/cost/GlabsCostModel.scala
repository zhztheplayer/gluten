package org.apache.gluten.planner.cost

import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.enumerated.RemoveFilter
import org.apache.gluten.extension.columnar.enumerated.RemoveFilter.NoopFilter
import org.apache.gluten.extension.columnar.transition.{ColumnarToRowLike, RowToColumnarLike}
import org.apache.gluten.planner.plan.GlutenPlanModel.GroupLeafExec
import org.apache.gluten.ras.{Cost, CostModel}
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

class GlabsCostModel extends CostModel[SparkPlan] with Logging {
  import GlabsCostModel._
  private val infLongCost = Long.MaxValue

  logInfo(s"Created cost model: ${classOf[GlabsCostModel]}")

  override def costOf(node: SparkPlan): GlutenCost = node match {
    case _: GroupLeafExec => throw new IllegalStateException()
    case _ => GlutenCost(longCostOf(node))
  }

  private def longCostOf(node: SparkPlan): Long = node match {
    case n =>
      val selfCost = selfLongCostOf(n)

      // Sum with ceil to avoid overflow.
      def safeSum(a: Long, b: Long): Long = {
        assert(a >= 0)
        assert(b >= 0)
        val sum = a + b
        if (sum < a || sum < b) Long.MaxValue else sum
      }

      (n.children.map(longCostOf).toList :+ selfCost).reduce(safeSum)
  }

  // A plan next to scan may be able to push runtime filters to scan in Velox.
  // TODO: Check join build side
  private def isNextToScanTransformer(plan: SparkPlan): Boolean = {
    plan.children.exists {
      case NoopFilter(_: BasicScanExecTransformer, _) => true
      case _: BasicScanExecTransformer => true
      case _ => false
    }
  }

  // A very rough estimation as of now.
  private def selfLongCostOf(node: SparkPlan): Long = {
    node match {
      case _: ProjectExecTransformer => 0L
      case _: ProjectExec => 0L

      case _: ShuffleExchangeExec => 3L
      case _: VeloxResizeBatchesExec => 0L
      case _: ColumnarShuffleExchangeExec => 2L

      // Consider joins, aggregations, windows to be highest priority for Gluten to offload.
      case _: BroadcastHashJoinExec => 60L
      case j: BroadcastHashJoinExecTransformer if isNextToScanTransformer(j) => 32L
      case _: BroadcastHashJoinExecTransformer => 52L

      case _: ShuffledHashJoinExec => 80L
      case j: ShuffledHashJoinExecTransformer if isNextToScanTransformer(j) => 32L
      case _: ShuffledHashJoinExecTransformer => 52L

      case _: SortMergeJoinExec => 80L
      case j: SortMergeJoinExecTransformer if isNextToScanTransformer(j) => 32L
      case _: SortMergeJoinExecTransformer => 52L

      case _: HashAggregateExec => 80L
      case _: ObjectHashAggregateExec => 80L
      case _: SortAggregateExec => 80L
      case _: HashAggregateExecTransformer => 52L

      case _: WindowExec => 80L
      case _: WindowExecTransformer => 52L

      case r2c: RowToColumnarExecBase if hasComplexTypes(r2c.schema) =>
        // Avoid moving computation back to native when transition has complex types in schema.
        // Such transitions are observed to be extremely expensive as of now.
        Long.MaxValue

      // Row-to-Velox is observed much more expensive than Velox-to-row.
      case ColumnarToRowExec(child) => 2L
      case RowToColumnarExec(child) => 15L
      case ColumnarToRowLike(child) => 2L
      case RowToColumnarLike(child) => 15L

      case _: RemoveFilter.NoopFilter =>
        // To make planner choose the tree that has applied rule PushFilterToScan.
        0L

      case p if PlanUtil.isGlutenColumnarOp(p) => 2L
      case p if PlanUtil.isVanillaColumnarOp(p) => 3L
      // Other row ops. Usually a vanilla row op.
      case _ => 5L
    }
  }

  private def isCheapExpression(ne: NamedExpression): Boolean = ne match {
    case Alias(_: Attribute, _) => true
    case _: Attribute => true
    case _ => false
  }

  private def hasComplexTypes(schema: StructType): Boolean = {
    schema.exists(_.dataType match {
      case _: StructType => true
      case _: ArrayType => true
      case _: MapType => true
      case _ => false
    })
  }

  override def costComparator(): Ordering[Cost] = Ordering.Long.on {
    case GlutenCost(value) => value
    case _ => throw new IllegalStateException("Unexpected cost type")
  }

  override def makeInfCost(): Cost = GlutenCost(infLongCost)
}

object GlabsCostModel {
  case class GlutenCost(value: Long) extends Cost
}
