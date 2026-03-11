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
package org.apache.gluten.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/** Unwraps star-schema wrapper aggregates (A/B/C/D) back to original Spark aggregates. */
case class UnwrapStarSchemaWrapperAggregate(session: SparkSession) extends Rule[SparkPlan] {
  import StarSchemaAggregateWrapper._

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case agg: HashAggregateExec if hasWrapper(agg) =>
        rewriteHashAggregate(agg)
    }
  }

  private def hasWrapper(agg: HashAggregateExec): Boolean = {
    agg.aggregateExpressions.exists {
      ae =>
        ae.aggregateFunction match {
          case _: StarSchemaAggregateWrapper => true
          case _ => false
        }
    }
  }

  private def rewriteHashAggregate(agg: HashAggregateExec): SparkPlan = {
    val childAdaptations = ArrayBuffer.empty[NamedExpression]
    val outputMappings = ArrayBuffer.empty[(Attribute, Expression)]

    val rewrittenAggExprs = agg.aggregateExpressions.map {
      ae =>
        ae.aggregateFunction match {
          case w: StarSchemaAggregateWrapper =>
            val rewrittenMode = semanticMode(ae.mode, w.targetPhase)
            val rewritten = ae.copy(aggregateFunction = w.innerAgg, mode = rewrittenMode)
            if (isWrapperC(ae.mode, w.targetPhase)) {
              val payload = w.children.head
              val targets = rewritten.aggregateFunction.inputAggBufferAttributes
              childAdaptations ++= expandPayload(payload, targets, agg.child.output)
            }
            outputMappings ++= buildOutputMappings(ae, rewritten, w.targetPhase)
            rewritten
          case _ =>
            ae
        }
    }

    val rewrittenChild = if (childAdaptations.nonEmpty) {
      val base = agg.child.output.map(a => Alias(a, a.name)(exprId = a.exprId))
      val dedup = childAdaptations.foldLeft(Seq.empty[NamedExpression]) {
        case (acc, ne) if acc.exists(_.exprId == ne.exprId) => acc
        case (acc, ne) => acc :+ ne
      }
      ProjectExec(base ++ dedup, agg.child)
    } else {
      agg.child
    }

    val rewrittenAgg = agg.copy(aggregateExpressions = rewrittenAggExprs, child = rewrittenChild)
    val projected = agg.output.map {
      case a: AttributeReference =>
        val rewrittenExpr = rewriteAttr(a, outputMappings.toSeq)
        Alias(rewrittenExpr, a.name)(exprId = a.exprId, qualifier = a.qualifier)
      case other =>
        val rewrittenExpr = rewriteAttr(other, outputMappings.toSeq)
        Alias(rewrittenExpr, other.name)(exprId = other.exprId, qualifier = other.qualifier)
    }
    ProjectExec(projected, rewrittenAgg)
  }

  private def rewriteAttr(expr: Expression, pairs: Seq[(Attribute, Expression)]): Expression = {
    expr.transformUp {
      case a: Attribute =>
        pairs
          .collectFirst {
            case (from, to) if a.semanticEquals(from) => to
          }
          .getOrElse(a)
    }
  }

  private def outputAttrs(ae: AggregateExpression): Seq[Attribute] = {
    ae.mode match {
      case Partial | PartialMerge => ae.aggregateFunction.aggBufferAttributes
      case Final | Complete => Seq(ae.resultAttribute)
      case _ => Seq(ae.resultAttribute)
    }
  }

  private def expandPayload(
      payload: Expression,
      targets: Seq[AttributeReference],
      childOutput: Seq[Attribute]): Seq[NamedExpression] = {
    val childOutputSet = AttributeSet(childOutput)
    val payloadResolvable = payload.references.forall {
      ref => childOutputSet.exists(_.exprId == ref.exprId)
    }

    if (!payloadResolvable) {
      targets.map {
        target =>
          val source = childOutput.find(_.name == target.name).getOrElse {
            throw new IllegalStateException(
              s"Cannot rebind wrapper payload for target ${target.name} from child output.")
          }
          Alias(source, target.name)(exprId = target.exprId)
      }
    } else if (targets.size == 1 && !payload.dataType.isInstanceOf[StructType]) {
      val target = targets.head
      val source = childOutput.find(_.name == target.name).getOrElse(payload)
      Seq(Alias(source, target.name)(exprId = target.exprId))
    } else {
      payload.dataType match {
        case _: StructType =>
          targets.zipWithIndex.map {
            case (target, idx) =>
              Alias(GetStructField(payload, idx), target.name)(exprId = target.exprId)
          }
        case _ =>
          targets.map {
            target =>
              val source = childOutput.find(_.name == target.name).getOrElse {
                throw new IllegalStateException(
                  s"Cannot rebind wrapper payload for target ${target.name} from child output.")
              }
              Alias(source, target.name)(exprId = target.exprId)
          }
      }
    }
  }

  private def buildOutputMappings(
      original: AggregateExpression,
      rewritten: AggregateExpression,
      targetPhase: TargetPhase): Seq[(Attribute, Expression)] = {
    val originalOutputs = outputAttrs(original)
    val rewrittenOutputs = outputAttrs(rewritten)
    if (isWrapperB(original.mode, targetPhase)) {
      Seq(originalOutputs.head -> CreateStruct(rewrittenOutputs))
    } else {
      require(
        originalOutputs.size == rewrittenOutputs.size,
        s"Wrapper output size mismatch: original=${originalOutputs.size}, " +
          s"rewritten=${rewrittenOutputs.size}")
      originalOutputs.zip(rewrittenOutputs)
    }
  }

  private def isWrapperB(mode: AggregateMode, targetPhase: TargetPhase): Boolean = {
    mode == Final && targetPhase == PartialPhase
  }

  private def isWrapperC(mode: AggregateMode, targetPhase: TargetPhase): Boolean = {
    mode == Partial && targetPhase == FinalPhase
  }

}
