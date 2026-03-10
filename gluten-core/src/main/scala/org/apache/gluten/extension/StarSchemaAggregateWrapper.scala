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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, CreateStruct, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.types.DataType

import java.util.Locale

import scala.collection.mutable

object StarSchemaAggregateWrapper {
  sealed trait WrapperStage {
    def sqlName: String
  }

  case object WrapperPartialStage extends WrapperStage {
    override val sqlName: String = "WRAPPER_PARTIAL"
  }

  case object WrapperFinalStage extends WrapperStage {
    override val sqlName: String = "WRAPPER_FINAL"
  }

  sealed trait TargetPhase {
    def sqlName: String
  }

  case object PartialPhase extends TargetPhase {
    override val sqlName: String = "PARTIAL"
  }

  case object FinalPhase extends TargetPhase {
    override val sqlName: String = "FINAL"
  }

  def wrapperPartial(
      innerAgg: DeclarativeAggregate,
      targetPhase: TargetPhase): StarSchemaAggregateWrapper = {
    StarSchemaAggregateWrapper(
      innerAgg = innerAgg,
      wrapperStage = WrapperPartialStage,
      targetPhase = targetPhase,
      inputBuffer = None)
  }

  def wrapperFinal(
      innerAgg: DeclarativeAggregate,
      targetPhase: TargetPhase,
      inputBuffer: Expression): StarSchemaAggregateWrapper = {
    StarSchemaAggregateWrapper(
      innerAgg = innerAgg,
      wrapperStage = WrapperFinalStage,
      targetPhase = targetPhase,
      inputBuffer = Some(inputBuffer))
  }
}

case class StarSchemaAggregateWrapper(
    innerAgg: DeclarativeAggregate,
    wrapperStage: StarSchemaAggregateWrapper.WrapperStage,
    targetPhase: StarSchemaAggregateWrapper.TargetPhase,
    inputBuffer: Option[Expression])
  extends DeclarativeAggregate {
  import StarSchemaAggregateWrapper._

  private val wrappedBufferAttrs: Seq[AttributeReference] =
    innerAgg.aggBufferAttributes.zipWithIndex.map {
      case (attr, index) =>
        AttributeReference(
          s"ss_wrapper_buf_${targetPhase.sqlName.toLowerCase(Locale.ROOT)}_$index",
          attr.dataType,
          attr.nullable)()
    }

  private def outputBufferExpr: Expression =
    inputBuffer.getOrElse(CreateStruct(innerAgg.inputAggBufferAttributes))

  override lazy val nullable: Boolean = true

  override lazy val dataType: DataType = wrapperStage match {
    case WrapperPartialStage =>
      CreateStruct(wrappedBufferAttrs).dataType
    case WrapperFinalStage =>
      innerAgg.dataType
  }

  override def children: Seq[Expression] = wrapperStage match {
    case WrapperPartialStage => innerAgg.children
    case WrapperFinalStage => Seq(outputBufferExpr)
  }

  override lazy val aggBufferAttributes: Seq[AttributeReference] = wrappedBufferAttrs

  override lazy val initialValues: Seq[Expression] = {
    rewrite(innerAgg.initialValues, childReplacements = Map.empty, useInputBufferField = false)
  }

  override lazy val updateExpressions: Seq[Expression] = targetPhase match {
    case PartialPhase =>
      rewrite(
        innerAgg.updateExpressions,
        childReplacements = innerAgg.children.zip(children).toMap,
        useInputBufferField = false
      )
    case FinalPhase =>
      rewrite(innerAgg.mergeExpressions, childReplacements = Map.empty, useInputBufferField = true)
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    rewrite(innerAgg.mergeExpressions, childReplacements = Map.empty, useInputBufferField = false)
  }

  override lazy val evaluateExpression: Expression = wrapperStage match {
    case WrapperPartialStage =>
      CreateStruct(aggBufferAttributes)
    case WrapperFinalStage =>
      rewrite(
        innerAgg.evaluateExpression,
        childReplacements = Map.empty,
        useInputBufferField = false)
  }

  override def nodeName: String = "StarSchemaAggregateWrapper"

  override def prettyName: String =
    s"ss_agg_wrapper_${wrapperStage.sqlName.toLowerCase(Locale.ROOT)}_" +
      s"${targetPhase.sqlName.toLowerCase(Locale.ROOT)}"

  override def sql: String = {
    s"$prettyName(${innerAgg.sql(false)})"
  }

  override lazy val deterministic: Boolean = innerAgg.deterministic

  override lazy val defaultResult: Option[Literal] = wrapperStage match {
    case WrapperPartialStage => None
    case WrapperFinalStage => innerAgg.defaultResult
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    wrapperStage match {
      case WrapperPartialStage =>
        val newInner = innerAgg.withNewChildren(newChildren).asInstanceOf[DeclarativeAggregate]
        copy(innerAgg = newInner, inputBuffer = None)
      case WrapperFinalStage =>
        if (newChildren.size != 1) {
          throw new IllegalArgumentException(
            s"FINAL StarSchemaAggregateWrapper expects exactly one child, got ${newChildren.size}")
        }
        copy(inputBuffer = Some(newChildren.head))
    }
  }

  private def rewrite(
      exprs: Seq[Expression],
      childReplacements: Map[Expression, Expression],
      useInputBufferField: Boolean): Seq[Expression] = {
    exprs.map(rewrite(_, childReplacements, useInputBufferField))
  }

  private def rewrite(
      expr: Expression,
      childReplacements: Map[Expression, Expression],
      useInputBufferField: Boolean): Expression = {
    val innerToWrappedBuffer = innerAgg.aggBufferAttributes.zip(aggBufferAttributes)
    val innerToInputBuffer = innerAgg.inputAggBufferAttributes.zipWithIndex.map {
      case (attr, index) =>
        if (useInputBufferField) {
          attr -> GetStructField(outputBufferExpr, index, Some(attr.name))
        } else {
          attr -> inputAggBufferAttributes(index)
        }
    }
    val attrRewriteMap = mutable.ArrayBuffer.empty[(Attribute, Expression)]
    attrRewriteMap ++= innerToWrappedBuffer
    attrRewriteMap ++= innerToInputBuffer
    val childRewriteSeq = childReplacements.toSeq

    childRewriteSeq
      .foldLeft(expr) {
        case (curExpr, (from, to)) =>
          curExpr.transformUp {
            case e if e.semanticEquals(from) => to
          }
      }
      .transformUp {
        case a: Attribute =>
          attrRewriteMap
            .collectFirst {
              case (from, to) if a.semanticEquals(from) => to
            }
            .getOrElse(a)
      }
  }
}
