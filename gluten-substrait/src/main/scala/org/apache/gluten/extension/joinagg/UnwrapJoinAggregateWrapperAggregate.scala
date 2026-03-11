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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateStruct, ExprId, Expression, GetStructField, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/**
 * Unwraps JoinAggregateFunctionWrapper for one adjacent aggregate pair.
 *
 * Design:
 * - match only HashAggregateExec(parent) + HashAggregateExec(child)
 * - unwrap child first, then parent
 * - never insert a middle Project between parent and child while unwrapping the pair
 */
case class UnwrapJoinAggregateWrapperAggregate() extends Rule[SparkPlan] {
  import org.apache.gluten.extension.joinagg.JoinAggregateFunctionWrapper._

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableJoinAggregateRules) {
      return plan
    }
    plan match {
      case parent @ HashAggregateExec(_, _, _, _, _, _, _, _, child: HashAggregateExec)
          if hasWrapper(parent) || hasWrapper(child) =>
        rewritePair(parent, child)
      case _ =>
        plan
    }
  }

  private def rewritePair(parent: HashAggregateExec, child: HashAggregateExec): SparkPlan = {
    val rewrittenChild = rewriteSingle(child, allowMiddleProject = true)
    val rewrittenParentInput = parent.copy(child = rewrittenChild)
    rewriteSingle(rewrittenParentInput, allowMiddleProject = false)
  }

  private def hasWrapper(agg: HashAggregateExec): Boolean = {
    agg.aggregateExpressions.exists {
      _.aggregateFunction.isInstanceOf[JoinAggregateFunctionWrapper]
    }
  }

  private def rewriteSingle(agg: HashAggregateExec, allowMiddleProject: Boolean): HashAggregateExec = {
    if (!hasWrapper(agg)) {
      return agg
    }

    val childAdaptations = ArrayBuffer.empty[NamedExpression]
    val exprIdRewrite = collection.mutable.HashMap.empty[ExprId, Expression]

    val rewrittenAggExprs = agg.aggregateExpressions.map {
      ae =>
        ae.aggregateFunction match {
          case w: JoinAggregateFunctionWrapper =>
            val rewrittenMode = semanticMode(ae.mode, w.targetPhase)
            val rewritten = ae.copy(aggregateFunction = w.innerAgg, mode = rewrittenMode)

            if (isWrapperB(ae.mode, w.targetPhase) || isWrapperD(ae.mode, w.targetPhase)) {
              val oldIter = ae.aggregateFunction.inputAggBufferAttributes.iterator
              val newIter = rewritten.aggregateFunction.inputAggBufferAttributes.iterator
              while (oldIter.hasNext && newIter.hasNext) {
                val oldAttr = oldIter.next()
                val newAttr = newIter.next()
                val source = findSourceAttr(
                  agg.child.output,
                  oldAttr.asInstanceOf[AttributeReference],
                  newAttr)
                if (allowMiddleProject) {
                  childAdaptations += Alias(source, newAttr.name)(exprId = newAttr.exprId)
                } else {
                  exprIdRewrite += newAttr.exprId -> source
                }
              }
            }

            if (isWrapperC(ae.mode, w.targetPhase)) {
              val payload = w.children.head
              val targets = rewritten.aggregateFunction.inputAggBufferAttributes
              if (targets.size == 1 && !payload.dataType.isInstanceOf[StructType]) {
                val target = targets.head
                childAdaptations += Alias(payload, target.name)(exprId = target.exprId)
              } else {
                targets.zipWithIndex.foreach {
                  case (target, idx) =>
                    childAdaptations +=
                      Alias(GetStructField(payload, idx), target.name)(exprId = target.exprId)
                }
              }
            }
            rewritten
          case _ =>
            ae
        }
    }

    val rewrittenChild = if (childAdaptations.nonEmpty && allowMiddleProject) {
      val projectedExprIds = collection.mutable.HashSet.empty[ExprId]
      val base = agg.child.output.map { a =>
        projectedExprIds += a.exprId
        Alias(a, a.name)(exprId = a.exprId)
      }
      val append = childAdaptations.filterNot(ne => projectedExprIds.contains(ne.exprId))
      ProjectExec(base ++ append, agg.child)
    } else {
      agg.child
    }

    val rewrittenAggregateAttributes = rewrittenAggExprs.flatMap {
      ae =>
        ae.mode match {
          case Partial | PartialMerge => ae.aggregateFunction.aggBufferAttributes
          case Final | Complete => Seq(ae.resultAttribute)
          case _ => Seq(ae.resultAttribute)
        }
    }

    val outputRewrites =
      buildOutputRewrites(agg, rewrittenAggExprs, rewrittenAggregateAttributes, exprIdRewrite.toMap)

    val rewrittenResultExpressions = agg.resultExpressions.map { re =>
      rewriteExprByExprId(re, outputRewrites) match {
        case ne: NamedExpression => ne
        case other => Alias(other, re.name)(exprId = re.exprId, qualifier = re.qualifier)
      }
    }

    val rewrittenRequiredDist = rewrittenRequiredChildDistribution(agg)
      .map(_.map(expr => rewriteExprByExprId(expr, exprIdRewrite.toMap)))

    val rewrittenGrouping = agg.groupingExpressions
      .map(expr => rewriteExprByExprId(expr, exprIdRewrite.toMap).asInstanceOf[NamedExpression])
    val rewrittenAggExprsFinal = rewrittenAggExprs
      .map(ae => rewriteExprByExprId(ae, exprIdRewrite.toMap).asInstanceOf[AggregateExpression])

    agg.copy(
      requiredChildDistributionExpressions = rewrittenRequiredDist,
      groupingExpressions = rewrittenGrouping,
      aggregateExpressions = rewrittenAggExprsFinal,
      aggregateAttributes = rewrittenAggregateAttributes,
      resultExpressions = rewrittenResultExpressions,
      child = rewrittenChild
    )
  }

  private def buildOutputRewrites(
      originalAgg: HashAggregateExec,
      rewrittenAggExprs: Seq[AggregateExpression],
      rewrittenAggregateAttributes: Seq[Attribute],
      baseRewrites: Map[ExprId, Expression]): Map[ExprId, Expression] = {
    val rewrites = collection.mutable.HashMap.empty[ExprId, Expression]
    rewrites ++= baseRewrites

    var oldCursor = 0
    var newCursor = 0

    originalAgg.aggregateExpressions.zip(rewrittenAggExprs).foreach {
      case (oldAe, newAe) =>
        val oldCount = outputAttrCount(oldAe)
        val newCount = outputAttrCount(newAe)
        val oldSlice = originalAgg.aggregateAttributes.slice(oldCursor, oldCursor + oldCount)
        val newSlice = rewrittenAggregateAttributes.slice(newCursor, newCursor + newCount)

        oldSlice.zip(newSlice).foreach {
          case (oldAttr, newAttr) => rewrites += oldAttr.exprId -> newAttr
        }

        oldAe.aggregateFunction match {
          case w: JoinAggregateFunctionWrapper
              if isWrapperB(oldAe.mode, w.targetPhase) && oldSlice.nonEmpty =>
            val payloadExpr: Expression = oldSlice.head.dataType match {
              case st: StructType if st.fields.length == newSlice.length =>
                CreateStruct(newSlice.zip(st.fields).map { case (a, f) => Alias(a, f.name)() })
              case _ =>
                CreateStruct(newSlice)
            }
            rewrites += oldSlice.head.exprId -> payloadExpr
            rewrites += oldAe.resultAttribute.exprId -> payloadExpr
          case _ =>
            rewrites += oldAe.resultAttribute.exprId -> newAe.resultAttribute
        }

        oldCursor += oldCount
        newCursor += newCount
    }
    rewrites.toMap
  }

  private def outputAttrCount(ae: AggregateExpression): Int = {
    ae.mode match {
      case Partial | PartialMerge => ae.aggregateFunction.aggBufferAttributes.size
      case Final | Complete => 1
      case _ => 1
    }
  }

  private def rewriteExprByExprId(expr: Expression, rewrites: Map[ExprId, Expression]): Expression = {
    expr.transformUp {
      case a: Attribute => rewrites.getOrElse(a.exprId, a)
    }
  }

  private def findSourceAttr(
      childOutput: Seq[Attribute],
      oldAttr: AttributeReference,
      newAttr: Attribute): Attribute = {
    childOutput
      .find(_.exprId == oldAttr.exprId)
      .orElse(childOutput.find(_.exprId == newAttr.exprId))
      .orElse {
        val byNameType = childOutput.filter(a => a.name == oldAttr.name && a.dataType == oldAttr.dataType)
        if (byNameType.size == 1) Some(byNameType.head) else None
      }
      .orElse {
        val byNameType = childOutput.filter(a => a.name == newAttr.name && a.dataType == newAttr.dataType)
        if (byNameType.size == 1) Some(byNameType.head) else None
      }
      .getOrElse {
        throw new IllegalStateException(
          s"Cannot resolve wrapper input ${oldAttr.name}#${oldAttr.exprId.id}/" +
            s"${newAttr.name}#${newAttr.exprId.id} from child output " +
            childOutput.map(a => s"${a.name}#${a.exprId.id}").mkString("[", ", ", "]"))
      }
  }

  private def isWrapperB(mode: AggregateMode, targetPhase: TargetPhase): Boolean = {
    mode == Final && targetPhase == PartialPhase
  }

  private def isWrapperC(mode: AggregateMode, targetPhase: TargetPhase): Boolean = {
    mode == Partial && targetPhase == FinalPhase
  }

  private def isWrapperD(mode: AggregateMode, targetPhase: TargetPhase): Boolean = {
    mode == Final && targetPhase == FinalPhase
  }

  private def rewrittenRequiredChildDistribution(agg: HashAggregateExec): Option[Seq[Expression]] = {
    val hasWrapperB = agg.aggregateExpressions.exists {
      case AggregateExpression(w: JoinAggregateFunctionWrapper, mode, _, _, _) =>
        isWrapperB(mode, w.targetPhase)
      case _ => false
    }
    val hasWrapperC = agg.aggregateExpressions.exists {
      case AggregateExpression(w: JoinAggregateFunctionWrapper, mode, _, _, _) =>
        isWrapperC(mode, w.targetPhase)
      case _ => false
    }
    if (hasWrapperB) {
      None
    } else if (hasWrapperC) {
      Some(agg.groupingExpressions)
    } else {
      agg.requiredChildDistributionExpressions
    }
  }
}
