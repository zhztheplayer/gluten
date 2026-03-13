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

package org.apache.gluten.extension.joinagg

import org.apache.gluten.config.GlutenConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateStruct, GetStructField, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

import scala.collection.mutable.ArrayBuffer

case class ImplementJoinAggregate(spark: SparkSession) extends SparkStrategy {
  /*
   * This strategy lowers the logical join-aggregate wrapper shape produced by
   * `PushAggregateThroughJoin` into ordinary Spark physical hash aggregates.
   *
   * The optimizer builds two wrapper phases:
   *   - pushed phase: wrapper partial below / through joins
   *   - final phase: wrapper final above the pushed phase
   *
   * Spark's physical aggregate operators still expect normal aggregate functions and normal input
   * / buffer attributes. This strategy therefore:
   *   1. rewrites wrapper aggregates back to the wrapped Spark aggregate with the correct physical
   *      aggregate mode;
   *   2. inserts a post-project for the pushed phase to pack Spark aggregate buffers into a
   *      struct, matching the wrapper's logical output type;
   *   3. inserts a pre-project for the final phase to unpack that struct back into the buffer
   *      attributes expected by the wrapped Spark aggregate.
   *
   * In short: the wrapper exists only at the logical boundary; this strategy makes the plan look
   * like an ordinary Spark aggregate plan again while preserving the wrapper data contract across
   * the join.
   */

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!GlutenConfig.get.pushAggregateThroughJoinEnabled) {
      return Nil
    }

    plan match {
      case agg: Aggregate if containsWrapperAggregate(agg) =>
        planJoinAggregate(agg).toSeq
      case _ =>
        Nil
    }
  }

  private def planJoinAggregate(agg: Aggregate): Option[SparkPlan] = {
    // A single logical aggregate must lower either entirely as pushed-phase wrappers or entirely
    // as final-phase wrappers. Mixed-phase aggregates are rejected here.
    val grouping = agg.groupingExpressions.collect { case ne: NamedExpression => ne }
    if (grouping.size != agg.groupingExpressions.size) {
      return None
    }

    val wrappers = collectWrapperAggregateExpressions(agg.aggregateExpressions)
    if (wrappers.isEmpty) {
      return None
    }

    val phases = wrappers.map(_._2.targetPhase).distinct
    if (phases.size != 1) {
      return None
    }
    val phase = phases.head

    val childPlan = planLater(agg.child)
    phase match {
      case JoinAggregateFunctionWrapper.PartialPhase =>
        planPartialPhase(agg, grouping, childPlan)
      case JoinAggregateFunctionWrapper.FinalPhase =>
        planFinalPhase(agg, grouping, childPlan)
    }
  }

  private def planPartialPhase(
      agg: Aggregate,
      grouping: Seq[NamedExpression],
      childPlan: SparkPlan): Option[SparkPlan] = {
    // Lower the pushed wrapper phase as a normal Spark aggregate. Its physical output is the
    // wrapped aggregate buffer attributes, which are then packed into a struct so the downstream
    // final wrapper still sees the wrapper's logical row shape.
    val rewrittenOutput = rewriteAggregateOutput(agg.aggregateExpressions)
    val rewrittenAggExprs = collectAggregateExpressions(rewrittenOutput)
    if (rewrittenAggExprs.isEmpty) {
      return None
    }

    val hashAgg = HashAggregateExec(
      requiredChildDistributionExpressions = None,
      isStreaming = false,
      numShufflePartitions = None,
      groupingExpressions = grouping,
      aggregateExpressions = rewrittenAggExprs,
      aggregateAttributes = rewrittenAggExprs.flatMap(_.aggregateFunction.aggBufferAttributes),
      initialInputBufferOffset = 0,
      resultExpressions = grouping.map(_.toAttribute) ++ rewrittenAggExprs.flatMap(
        _.aggregateFunction.aggBufferAttributes),
      child = childPlan)

    val postProjectList = agg.aggregateExpressions.map {
      case alias @ Alias(ae: AggregateExpression, _) =>
        ae.aggregateFunction match {
          case _: JoinAggregateFunctionWrapper =>
            val rewrittenAe = findRewrittenAggregateExpr(ae, rewrittenAggExprs)
            val packed = CreateStruct(rewrittenAe.aggregateFunction.aggBufferAttributes)
            Alias(packed, alias.name)(
              exprId = alias.exprId,
              qualifier = alias.qualifier,
              explicitMetadata = alias.explicitMetadata,
              nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
            )
          case _ =>
            alias
        }
      case other =>
        other
    }

    Some(ProjectExec(postProjectList, hashAgg))
  }

  private def planFinalPhase(
      agg: Aggregate,
      grouping: Seq[NamedExpression],
      childPlan: SparkPlan): Option[SparkPlan] = {
    // Lower the final wrapper phase by first unpacking the wrapper struct into the wrapped
    // aggregate's input buffer attributes, then running a normal Spark final / merge aggregate.
    val rewrittenOutput = rewriteAggregateOutput(agg.aggregateExpressions)
    val wrapperWithRewritten = collectWrapperAggregateExpressions(agg.aggregateExpressions).map {
      case (originalAe, wrapper) =>
        val rewrittenAe =
          findRewrittenAggregateExpr(originalAe, collectAggregateExpressions(rewrittenOutput))
        (wrapper, rewrittenAe)
    }

    val unpackAliases = ArrayBuffer.empty[Alias]
    val seenExprIds = scala.collection.mutable.HashSet.empty[Long]
    wrapperWithRewritten.foreach {
      case (wrapper, rewrittenAe) =>
        val bufferExpr = wrapper.children.head
        rewrittenAe.aggregateFunction.inputAggBufferAttributes.zipWithIndex.foreach {
          case (bufferAttr, idx) if seenExprIds.add(bufferAttr.exprId.id) =>
            // Keep exprId for binding correctness, but avoid dotted names (e.g. a.b)
            // in the temporary unpack projection to prevent nested-field style mis-binding.
            val safeName = s"_joinagg_buf_${bufferAttr.exprId.id}_$idx"
            unpackAliases += Alias(
              GetStructField(bufferExpr, idx, Some(bufferAttr.name)),
              safeName
            )(exprId = bufferAttr.exprId, qualifier = bufferAttr.qualifier)
          case _ =>
        }
    }

    val childWithUnpacked = if (unpackAliases.nonEmpty) {
      ProjectExec(childPlan.output ++ unpackAliases, childPlan)
    } else {
      childPlan
    }

    val rewrittenAggExprs = collectAggregateExpressions(rewrittenOutput)
    if (rewrittenAggExprs.isEmpty) {
      return None
    }
    val aggregateAttrs = rewrittenAggExprs.map(_.resultAttribute)
    val hashAggResultExprs = rewriteResultAsAggregateAttributes(rewrittenOutput, rewrittenAggExprs)

    Some(
      HashAggregateExec(
        requiredChildDistributionExpressions = Some(grouping.map(_.toAttribute)),
        isStreaming = false,
        numShufflePartitions = None,
        groupingExpressions = grouping,
        aggregateExpressions = rewrittenAggExprs,
        aggregateAttributes = aggregateAttrs,
        initialInputBufferOffset = 0,
        resultExpressions = hashAggResultExprs,
        child = childWithUnpacked))
  }

  private def containsWrapperAggregate(agg: Aggregate): Boolean = {
    agg.aggregateExpressions.exists {
      _.exists {
        case AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _) => true
        case _ => false
      }
    }
  }

  private def collectWrapperAggregateExpressions(
      output: Seq[NamedExpression]): Seq[(AggregateExpression, JoinAggregateFunctionWrapper)] = {
    // Deduplicate by AggregateExpression result id so the same wrapper expression referenced
    // multiple times in the output is lowered once.
    output
      .flatMap {
        _.collect {
          case ae @ AggregateExpression(
                wrapper: JoinAggregateFunctionWrapper,
                _,
                _,
                _,
                _) =>
            (ae, wrapper)
        }
      }
      .foldLeft(Seq.empty[(AggregateExpression, JoinAggregateFunctionWrapper)]) {
        case (acc, cur @ (ae, _)) if acc.exists(_._1.resultId == ae.resultId) => acc
        case (acc, cur) => acc :+ cur
      }
  }

  private def rewriteAggregateOutput(output: Seq[NamedExpression]): Seq[NamedExpression] = {
    // Replace wrapper aggregates with the wrapped Spark aggregate and the physical aggregate mode
    // implied by the wrapper phase.
    output.map {
      _.transformUp {
        case ae @ AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _) =>
          val mode = JoinAggregateFunctionWrapper.semanticMode(ae.mode, wrapper.targetPhase)
          ae.copy(aggregateFunction = wrapper.innerAgg, mode = mode)
      }.asInstanceOf[NamedExpression]
    }
  }

  private def collectAggregateExpressions(output: Seq[NamedExpression]): Seq[AggregateExpression] = {
    output
      .flatMap {
        _.collect {
          case ae: AggregateExpression => ae
        }
      }
      .foldLeft(Seq.empty[AggregateExpression]) {
        case (acc, ae) if acc.exists(_.resultId == ae.resultId) => acc
        case (acc, ae) => acc :+ ae
      }
  }

  private def findRewrittenAggregateExpr(
      original: AggregateExpression,
      rewritten: Seq[AggregateExpression]): AggregateExpression = {
    rewritten.find(_.resultId == original.resultId).getOrElse {
      throw new IllegalStateException(
        s"Cannot find rewritten aggregate expression for ${original.sql}")
    }
  }

  private def rewriteResultAsAggregateAttributes(
      rewrittenOutput: Seq[NamedExpression],
      rewrittenAggExprs: Seq[AggregateExpression]): Seq[NamedExpression] = {
    // After the HashAggregateExec is built, rewrite the original output tree so every aggregate
    // expression points at the corresponding physical aggregate result attribute.
    rewrittenOutput.map {
      _.transformUp {
        case ae: AggregateExpression =>
          rewrittenAggExprs
            .find(_.resultId == ae.resultId)
            .map(_.resultAttribute)
            .getOrElse(
              throw new IllegalStateException(
                s"Cannot resolve aggregate attribute for ${ae.sql}"))
      }.asInstanceOf[NamedExpression]
    }
  }
}
