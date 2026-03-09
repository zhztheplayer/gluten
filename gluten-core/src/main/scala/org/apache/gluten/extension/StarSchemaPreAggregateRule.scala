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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

case class StarSchemaPreAggregateRule(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PredicateHelper {
  private var successfulPushCount: Int = 0

  def resetSuccessfulPushCount(): Unit = {
    successfulPushCount = 0
  }

  def getSuccessfulPushCount: Int = successfulPushCount

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case agg @ Aggregate(groupingExprs, aggExprs, child) if hasCountOne(aggExprs) =>
      extractJoin(child)
        .flatMap {
          case (join, requiredAttrs, rebuild) =>
            buildPreAgg(join, groupingExprs, requiredAttrs).map {
              case (preAgg, partialCnt) =>
                val rewrittenAggExprs = aggExprs.map {
                  case alias @ Alias(countExpr, _) if isCountOne(countExpr) =>
                    val cleanPartialCnt =
                      AttributeReference(partialCnt.name, partialCnt.dataType, partialCnt.nullable)(
                        exprId = partialCnt.exprId,
                        qualifier = partialCnt.qualifier)
                    Alias(Sum(cleanPartialCnt).toAggregateExpression(), alias.name)(
                      exprId = alias.exprId,
                      qualifier = alias.qualifier,
                      explicitMetadata = alias.explicitMetadata,
                      nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys)
                  case other => other
                }
                val rewrittenJoin = join.copy(left = preAgg)
                successfulPushCount += 1
                agg.copy(aggregateExpressions = rewrittenAggExprs, child = rebuild(rewrittenJoin))
            }
        }
        .getOrElse(agg)
  }

  private def extractJoin(child: LogicalPlan): Option[(Join, Seq[Attribute], Join => LogicalPlan)] =
    child match {
      case join: Join if isInnerEquiJoin(join) => Some((join, Seq.empty, identity))
      case filter @ Filter(_, join: Join) if isInnerEquiJoin(join) =>
        val leftOutputSet = join.left.outputSet
        val requiredAttrs = filter.condition.references.toSeq.collect {
          case attr: Attribute if leftOutputSet.contains(attr) => attr
        }
        Some((join, requiredAttrs, (j: Join) => filter.copy(child = j)))
      case _ => None
    }

  private def isInnerEquiJoin(join: Join): Boolean = {
    join.joinType == Inner && join.hint == JoinHint.NONE && join.condition.exists {
      cond =>
        splitConjunctivePredicates(cond).exists {
          case EqualTo(_: Attribute, _: Attribute) => true
          case _ => false
        }
    }
  }

  private def buildPreAgg(
      join: Join,
      groupingExprs: Seq[Expression],
      requiredAttrs: Seq[Attribute]): Option[(Aggregate, Attribute)] = {
    val leftOutputSet = join.left.outputSet
    val leftGroupingAttrs = groupingExprs.collect {
      case attr: Attribute if leftOutputSet.contains(attr) => attr
    }
    val joinLeftKeys = join.condition.toSeq.flatMap(splitConjunctivePredicates).collect {
      case EqualTo(l: Attribute, r: Attribute)
          if leftOutputSet.contains(l) && !leftOutputSet.contains(r) =>
        l
      case EqualTo(l: Attribute, r: Attribute)
          if leftOutputSet.contains(r) && !leftOutputSet.contains(l) =>
        r
    }
    val groupingKeys = (leftGroupingAttrs ++ requiredAttrs ++ joinLeftKeys).distinct
    if (groupingKeys.isEmpty) {
      None
    } else {
      val preAgg = Aggregate(
        groupingExpressions = groupingKeys,
        aggregateExpressions = groupingKeys :+
          Alias(Count(Seq(Literal(1))).toAggregateExpression(), "partial_cnt")(),
        child = join.left
      )
      val partialCnt = preAgg.output.find(_.name == "partial_cnt").get
      Some((preAgg, partialCnt))
    }
  }

  private def hasCountOne(aggExprs: Seq[Expression]): Boolean = {
    aggExprs.exists {
      case Alias(expr, _) => isCountOne(expr)
      case expr => isCountOne(expr)
    }
  }

  private def isCountOne(expr: Expression): Boolean = {
    expr match {
      case ae: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression =>
        ae.aggregateFunction match {
          case Count(Seq(Literal(1, _))) => true
          case _ => false
        }
      case _ => false
    }
  }
}
