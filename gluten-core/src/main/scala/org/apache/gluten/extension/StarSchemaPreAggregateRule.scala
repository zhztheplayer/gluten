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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EqualTo, Expression, Literal, Multiply}
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
    case agg @ Aggregate(groupingExprs, aggExprs, child) if hasPushableAggExpr(aggExprs) =>
      extractJoin(child)
        .flatMap {
          case (join, filterCond, rebuild) =>
            Seq(JoinLeft, JoinRight).iterator
              .flatMap {
                side =>
                  buildPreAgg(join, groupingExprs, filterCond, side).flatMap {
                    case (preAgg, sideCnt) =>
                      rewriteAggregateExpressions(aggExprs, sideCnt, side.outputSet(join)).map {
                        rewrittenAggExprs =>
                          val rewrittenJoin = side.replace(join, preAgg)
                          successfulPushCount += 1
                          agg.copy(
                            aggregateExpressions = rewrittenAggExprs,
                            child = rebuild(rewrittenJoin))
                      }
                  }
              }
              .toSeq
              .headOption
        }
        .getOrElse(agg)
  }

  private def extractJoin(
      child: LogicalPlan): Option[(Join, Option[Expression], Join => LogicalPlan)] =
    child match {
      case join: Join if isInnerEquiJoin(join) => Some((join, None, identity))
      case filter @ Filter(_, join: Join) if isInnerEquiJoin(join) =>
        Some((join, Some(filter.condition), (j: Join) => filter.copy(child = j)))
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
      filterCond: Option[Expression],
      side: JoinSide): Option[(Aggregate, Attribute)] = {
    val sideOutputSet = side.outputSet(join)
    val sideGroupingAttrs = groupingExprs.collect {
      case attr: Attribute if sideOutputSet.contains(attr) => attr
    }
    val filterAttrs = filterCond.toSeq.flatMap(_.references.toSeq).collect {
      case attr: Attribute if sideOutputSet.contains(attr) => attr
    }
    val joinKeys = join.condition.toSeq.flatMap(splitConjunctivePredicates).collect {
      case EqualTo(l: Attribute, r: Attribute)
          if sideOutputSet.contains(l) && !sideOutputSet.contains(r) =>
        l
      case EqualTo(l: Attribute, r: Attribute)
          if sideOutputSet.contains(r) && !sideOutputSet.contains(l) =>
        r
    }
    val groupingKeys = (sideGroupingAttrs ++ filterAttrs ++ joinKeys).distinct
    if (groupingKeys.isEmpty) {
      None
    } else {
      val preAgg = Aggregate(
        groupingExpressions = groupingKeys,
        aggregateExpressions = groupingKeys :+
          Alias(Count(Seq(Literal(1))).toAggregateExpression(), side.cntName)(),
        child = side.plan(join)
      )
      val sideCnt = preAgg.output.find(_.name == side.cntName).get
      Some((preAgg, sideCnt))
    }
  }

  private def rewriteAggregateExpressions(
      aggExprs: Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression],
      sideCnt: Attribute,
      sideOutputSet: org.apache.spark.sql.catalyst.expressions.AttributeSet)
      : Option[Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]] = {
    val cleanSideCnt = cleanAttr(sideCnt)
    var rewritten = false
    val newAggExprs = aggExprs.map {
      case alias @ Alias(countExpr, _) if isCountOne(countExpr) =>
        rewritten = true
        Alias(Sum(cleanSideCnt).toAggregateExpression(), alias.name)(
          exprId = alias.exprId,
          qualifier = alias.qualifier,
          explicitMetadata = alias.explicitMetadata,
          nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys)
      case alias @ Alias(sumExpr, _) =>
        sumChild(sumExpr) match {
          case Some(childExpr) if canPushToSide(childExpr, sideOutputSet) =>
            rewritten = true
            Alias(Sum(Multiply(childExpr, cleanSideCnt)).toAggregateExpression(), alias.name)(
              exprId = alias.exprId,
              qualifier = alias.qualifier,
              explicitMetadata = alias.explicitMetadata,
              nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys)
          case _ => alias
        }
      case other => other
    }
    if (rewritten) Some(newAggExprs) else None
  }

  private def cleanAttr(attr: Attribute): AttributeReference = {
    AttributeReference(attr.name, attr.dataType, attr.nullable)(
      exprId = attr.exprId,
      qualifier = attr.qualifier)
  }

  private def canPushToSide(
      expr: Expression,
      sideOutputSet: org.apache.spark.sql.catalyst.expressions.AttributeSet): Boolean = {
    expr.references.forall(attr => !sideOutputSet.contains(attr))
  }

  private def hasPushableAggExpr(aggExprs: Seq[Expression]): Boolean = {
    aggExprs.exists {
      case Alias(expr, _) => isCountOne(expr) || isSumExpr(expr)
      case expr => isCountOne(expr) || isSumExpr(expr)
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

  private def isSumExpr(expr: Expression): Boolean = sumChild(expr).isDefined

  private def sumChild(expr: Expression): Option[Expression] = {
    expr match {
      case ae: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression =>
        ae.aggregateFunction match {
          case Sum(child, _) => Some(child)
          case _ => None
        }
      case _ => None
    }
  }

  sealed private trait JoinSide {
    def cntName: String
    def plan(join: Join): LogicalPlan
    def outputSet(join: Join): org.apache.spark.sql.catalyst.expressions.AttributeSet
    def replace(join: Join, newPlan: LogicalPlan): Join
  }

  private case object JoinLeft extends JoinSide {
    override val cntName: String = "partial_cnt"
    override def plan(join: Join): LogicalPlan = join.left
    override def outputSet(join: Join): org.apache.spark.sql.catalyst.expressions.AttributeSet =
      join.left.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(left = newPlan)
  }

  private case object JoinRight extends JoinSide {
    override val cntName: String = "right_cnt"
    override def plan(join: Join): LogicalPlan = join.right
    override def outputSet(join: Join): org.apache.spark.sql.catalyst.expressions.AttributeSet =
      join.right.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(right = newPlan)
  }
}
