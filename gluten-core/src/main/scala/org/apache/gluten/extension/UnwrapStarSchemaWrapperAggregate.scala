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

    val rewrittenAggExprs = agg.aggregateExpressions.map {
      ae =>
        ae.aggregateFunction match {
          case w: StarSchemaAggregateWrapper =>
            val rewrittenMode = semanticMode(ae.mode, w.targetPhase)
            val rewritten = ae.copy(aggregateFunction = w.innerAgg, mode = rewrittenMode)
            if (isWrapperB(ae.mode, w.targetPhase)) {
              val oldInputs = ae.aggregateFunction.aggBufferAttributes
              val newInputs = rewritten.aggregateFunction.inputAggBufferAttributes
              childAdaptations ++= remapWrapperBuffers(oldInputs, newInputs, agg.child.output)
            }
            if (isWrapperC(ae.mode, w.targetPhase)) {
              val payload = w.children.head
              val targets = rewritten.aggregateFunction.inputAggBufferAttributes
              childAdaptations ++= expandPayload(payload, targets, agg.child.output)
            }
            if (isWrapperD(ae.mode, w.targetPhase)) {
              val oldInputs = ae.aggregateFunction.inputAggBufferAttributes
              val newInputs = rewritten.aggregateFunction.inputAggBufferAttributes
              childAdaptations ++= remapWrapperBuffers(oldInputs, newInputs, agg.child.output)
            }
            rewritten
          case _ =>
            ae
        }
    }

    val rewrittenChild = if (childAdaptations.nonEmpty) {
      val adaptedNameTypes = childAdaptations
        .map(ne => (ne.name, ne.dataType))
        .toSet
      val base = agg.child.output
        .filterNot(a => adaptedNameTypes.contains((a.name, a.dataType)))
        .map(a => Alias(a, a.name)(exprId = a.exprId))
      val dedup = childAdaptations.foldLeft(Seq.empty[NamedExpression]) {
        case (acc, ne) if acc.exists(_.exprId == ne.exprId) => acc
        case (acc, ne) => acc :+ ne
      }
      ProjectExec(base ++ dedup, agg.child)
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
    val preRewriteAgg = agg.copy(
      aggregateExpressions = rewrittenAggExprs,
      aggregateAttributes = rewrittenAggregateAttributes,
      child = rewrittenChild
    )
    val outputMappings = buildOutputMappingsByAggregateAttributes(agg, preRewriteAgg)
    val rewrittenResultExpressions = agg.resultExpressions.map {
      re =>
        val rewritten = rewriteAttr(re, outputMappings.toSeq)
        rewritten match {
          case ne: NamedExpression => ne
          case other =>
            Alias(other, re.name)(exprId = re.exprId, qualifier = re.qualifier)
        }
    }
    val rewrittenAgg = agg.copy(
      aggregateExpressions = rewrittenAggExprs,
      aggregateAttributes = rewrittenAggregateAttributes,
      resultExpressions = rewrittenResultExpressions,
      child = rewrittenChild
    )
    val result = rewrittenAgg
    result
  }

  private def buildOutputMappingsByAggregateAttributes(
      originalAgg: HashAggregateExec,
      rewrittenAgg: HashAggregateExec): Seq[(Attribute, Expression)] = {
    val mappings = ArrayBuffer.empty[(Attribute, Expression)]
    var originalCursor = 0
    var rewrittenCursor = 0

    originalAgg.aggregateExpressions.zip(rewrittenAgg.aggregateExpressions).foreach {
      case (originalAe, rewrittenAe) =>
        val originalCount = outputAttrCount(originalAe)
        val rewrittenCount = outputAttrCount(rewrittenAe)
        val originalSlice =
          originalAgg.aggregateAttributes.slice(originalCursor, originalCursor + originalCount)
        val rewrittenSlice =
          rewrittenAgg.aggregateAttributes.slice(rewrittenCursor, rewrittenCursor + rewrittenCount)

        originalAe.aggregateFunction match {
          case w: StarSchemaAggregateWrapper
              if isWrapperB(originalAe.mode, w.targetPhase) && originalSlice.nonEmpty =>
            val payloadExpr: Expression = originalSlice.head.dataType match {
              case st: StructType if st.fields.length == rewrittenSlice.length =>
                val renamed = rewrittenSlice.zip(st.fields).map {
                  case (attr, field) => Alias(attr, field.name)()
                }
                CreateStruct(renamed)
              case _ =>
                CreateStruct(rewrittenSlice)
            }
            mappings += originalSlice.head -> payloadExpr
            mappings += originalAe.resultAttribute -> payloadExpr
          case w: StarSchemaAggregateWrapper if isWrapperD(originalAe.mode, w.targetPhase) =>
            mappings ++= originalSlice.zip(rewrittenSlice)
            mappings += originalAe.resultAttribute -> rewrittenAe.resultAttribute
          case _: StarSchemaAggregateWrapper =>
            mappings ++= originalSlice.zip(rewrittenSlice)
            mappings += originalAe.resultAttribute -> rewrittenAe.resultAttribute
          case _ =>
          // Non-wrapper aggregate output attrs should keep their original binding.
        }
        originalCursor += originalCount
        rewrittenCursor += rewrittenCount
    }
    mappings.toSeq
  }

  private def outputAttrCount(ae: AggregateExpression): Int = {
    ae.mode match {
      case Partial | PartialMerge => ae.aggregateFunction.aggBufferAttributes.size
      case Final | Complete => 1
      case _ => 1
    }
  }

  private def rewriteAttr(expr: Expression, pairs: Seq[(Attribute, Expression)]): Expression = {
    expr.transformUp {
      case a: Attribute =>
        pairs
          .collectFirst {
            case (from, to) if a.semanticEquals(from) => to
            case (from: AttributeReference, to)
                if a.name == from.name && a.dataType == from.dataType =>
              to
          }
          .getOrElse(a)
    }
  }

  private def expandPayload(
      payload: Expression,
      targets: Seq[AttributeReference],
      childOutput: Seq[Attribute]): Seq[NamedExpression] = {
    val byChild = targets.map {
      target => resolveFromChild(target, childOutput).map(attr => target -> attr)
    }
    if (byChild.forall(_.isDefined)) {
      return byChild.map {
        case Some((target, source)) => Alias(source, target.name)(exprId = target.exprId)
        case None =>
          throw new IllegalStateException("Unreachable: byChild should be fully defined.")
      }
    }

    val childOutputSet = AttributeSet(childOutput)
    val payloadResolvable = payload.references.forall {
      ref => childOutputSet.exists(_.exprId == ref.exprId)
    }

    if (!payloadResolvable) {
      throw new IllegalStateException(
        s"Cannot rebind wrapper payload for targets ${targets.map(_.name).mkString(",")} " +
          s"from child output.")
    } else if (targets.size == 1 && !payload.dataType.isInstanceOf[StructType]) {
      val target = targets.head
      val source = resolveFromChild(target, childOutput).getOrElse {
        throw new IllegalStateException(
          s"Cannot resolve target ${target.name} from child output for scalar payload.")
      }
      Seq(Alias(source, target.name)(exprId = target.exprId))
    } else {
      payload.dataType match {
        case _: StructType =>
          targets.zipWithIndex.map {
            case (target, idx) =>
              Alias(GetStructField(payload, idx), target.name)(exprId = target.exprId)
          }
        case _ =>
          throw new IllegalStateException(
            s"Wrapper payload $payload cannot be expanded to ${targets.size} target attributes.")
      }
    }
  }

  private def resolveFromChild(
      target: AttributeReference,
      childOutput: Seq[Attribute]): Option[Attribute] = {
    childOutput.find(_.exprId == target.exprId).orElse {
      childOutput.find(a => a.name == target.name && a.dataType == target.dataType)
    }
  }

  private def remapWrapperBuffers(
      oldInputs: Seq[AttributeReference],
      newInputs: Seq[Attribute],
      childOutput: Seq[Attribute]): Seq[NamedExpression] = {
    oldInputs.zip(newInputs).map {
      case (oldAttr, newAttr) =>
        val source = childOutput
          .find(_.exprId == oldAttr.exprId)
          .orElse {
            childOutput.find(a => a.name == oldAttr.name && a.dataType == oldAttr.dataType)
          }
          .orElse {
            childOutput.find(_.exprId == newAttr.exprId)
          }
          .orElse {
            childOutput.find(a => a.name == newAttr.name && a.dataType == newAttr.dataType)
          }
          .getOrElse {
            throw new IllegalStateException(
              s"Cannot find wrapper buffer input ${oldAttr.name}/${newAttr.name} in child output.")
          }
        Alias(source, newAttr.name)(exprId = newAttr.exprId)
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

}
