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

import org.apache.gluten.config.GlutenConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, EqualTo, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.optimizer.DecimalAggregates
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

case class PushStarSchemaPreAggregate(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PredicateHelper {
  private case class SidePartialSpec(
      originalExpr: Expression,
      aggregate: DeclarativeAggregate,
      wrapperKey: String)

  private case class SidePartialRef(spec: SidePartialSpec, attr: AttributeReference)

  private case class PreAggInfo(plan: Aggregate, sidePartials: Seq[SidePartialRef])

  private var successfulPushCount: Int = 0

  def resetSuccessfulPushCount(): Unit = {
    successfulPushCount = 0
  }

  def getSuccessfulPushCount: Int = successfulPushCount

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isEnabled) {
      return plan
    }
    if (successfulPushCount > 0 || containsWrapperAggregate(plan)) {
      return plan
    }
    plan.transformUp {
      case agg @ Aggregate(groupingExprs, aggExprs, child) if hasPushableAggExpr(aggExprs) =>
        extractJoin(child)
          .flatMap {
            case (join, wrapperRequiredAttrs, rebuild) =>
              Seq(JoinLeft, JoinRight).iterator
                .flatMap {
                  side =>
                    buildPreAgg(join, groupingExprs, wrapperRequiredAttrs, aggExprs, side).flatMap {
                      preAggInfo =>
                        rewriteAggregateExpressions(aggExprs, preAggInfo.sidePartials).map {
                          rewrittenAggExprs =>
                            val rewrittenJoin = side.replace(join, preAggInfo.plan)
                            val requiredAttrs =
                              dedupeAttrs(
                                (groupingExprs ++ rewrittenAggExprs).flatMap(
                                  referencedAttrsInOrder) ++
                                  rewrittenAggExprs.flatMap(aggregateBufferAttrsInOrder))
                            successfulPushCount += 1
                            agg.copy(
                              aggregateExpressions = rewrittenAggExprs,
                              child = rebuild(rewrittenJoin, requiredAttrs)
                            )
                        }
                    }
                }
                .toSeq
                .headOption
          }
          .getOrElse(agg)
    }
  }

  private def isEnabled: Boolean = {
    GlutenConfig.get.enableStarSchemaJoinAggregateRules
  }

  private def containsWrapperAggregate(plan: LogicalPlan): Boolean = {
    plan.exists {
      case Aggregate(_, aggExprs, _) =>
        aggExprs.exists {
          case Alias(ae: AggregateExpression, _) =>
            ae.aggregateFunction.isInstanceOf[StarSchemaAggregateFunctionWrapper]
          case ae: AggregateExpression =>
            ae.aggregateFunction.isInstanceOf[StarSchemaAggregateFunctionWrapper]
          case _ =>
            false
        }
      case _ =>
        false
    }
  }

  private def extractJoin(
      child: LogicalPlan): Option[(Join, Seq[Attribute], (Join, Seq[Attribute]) => LogicalPlan)] =
    child match {
      case project @ Project(_, projectChild) =>
        extractJoin(projectChild).map {
          case (join, wrapperRequiredAttrs, rebuild) =>
            val projectRequiredAttrs =
              dedupeAttrs(
                project.projectList.flatMap(referencedAttrsInOrder) ++ wrapperRequiredAttrs)
            (
              join,
              projectRequiredAttrs,
              (j: Join, requiredAttrs: Seq[Attribute]) => {
                val rebuiltChild = rebuild(j, requiredAttrs)
                val projectOutputSet = AttributeSet(project.projectList.map(_.toAttribute))
                val passThroughAttrs = requiredAttrs.filter {
                  attr => rebuiltChild.outputSet.contains(attr) && !projectOutputSet.contains(attr)
                }
                project.copy(
                  projectList = project.projectList ++ dedupeAttrs(passThroughAttrs),
                  child = rebuiltChild
                )
              }
            )
        }
      case join: Join if isInnerEquiJoin(join) =>
        Some((join, Nil, (j: Join, _: Seq[Attribute]) => j))
      case filter @ Filter(_, join: Join) if isInnerEquiJoin(join) =>
        Some(
          (
            join,
            referencedAttrsInOrder(filter.condition),
            (j: Join, _: Seq[Attribute]) => filter.copy(child = j)))
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
      wrapperRequiredAttrs: Seq[Attribute],
      aggExprs: Seq[NamedExpression],
      side: JoinSide): Option[PreAggInfo] = {
    val sideOutputSet = side.outputSet(join)
    val groupingRefAttrs = groupingExprs.flatMap(referencedAttrsInOrder)
    val sideGroupingAttrs = groupingRefAttrs.collect {
      case attr: Attribute if sideOutputSet.contains(attr) => attr
    }
    val wrapperAttrs = wrapperRequiredAttrs.collect {
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
    val joinConditionAttrs = join.condition.toSeq
      .flatMap(referencedAttrsInOrder)
      .collect { case attr: Attribute if sideOutputSet.contains(attr) => attr }
    val groupingKeys =
      dedupeAttrs(sideGroupingAttrs ++ wrapperAttrs ++ joinKeys ++ joinConditionAttrs)
    if (groupingKeys.isEmpty) {
      None
    } else {
      val sidePartials = collectSidePartials(aggExprs, sideOutputSet)
      if (sidePartials.isEmpty) {
        None
      } else {
        val sidePartialAliases = sidePartials.zipWithIndex.map {
          case (spec, idx) =>
            Alias(
              StarSchemaAggregateFunctionWrapper
                .wrapperPartial(spec.aggregate, spec.wrapperKey)
                .toAggregateExpression(),
              s"${side.partialName}_$idx"
            )()
        }

        val preAgg = Aggregate(
          groupingExpressions = groupingKeys,
          aggregateExpressions = groupingKeys ++ sidePartialAliases,
          child = side.plan(join)
        )
        val sidePartialOutputAttrs =
          preAgg.output.drop(groupingKeys.size).take(sidePartials.size).map(cleanAttr)
        val sidePartialRefs = sidePartials.zip(sidePartialOutputAttrs).map {
          case (spec, attr) => SidePartialRef(spec, attr)
        }
        Some(PreAggInfo(preAgg, sidePartialRefs))
      }
    }
  }

  private def rewriteAggregateExpressions(
      aggExprs: Seq[NamedExpression],
      sidePartials: Seq[SidePartialRef]): Option[Seq[NamedExpression]] = {
    var rewrittenAny = false

    val rewrittenAggExprs = aggExprs.map {
      case alias @ Alias(expr, _) if isPushableExpr(expr) =>
        rewriteAggregateExpr(expr, sidePartials) match {
          case Some(rewrittenExpr) =>
            rewrittenAny = true
            Alias(rewrittenExpr, alias.name)(
              exprId = alias.exprId,
              qualifier = alias.qualifier,
              explicitMetadata = alias.explicitMetadata,
              nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
            )
          case None =>
            return None
        }

      case alias @ Alias(_, _) =>
        alias

      case other if isPushableExpr(other) =>
        return None

      case other =>
        other
    }

    if (rewrittenAny) Some(rewrittenAggExprs) else None
  }

  private def rewriteAggregateExpr(
      expr: Expression,
      sidePartials: Seq[SidePartialRef]): Option[Expression] = {
    pushableSpec(expr).flatMap {
      spec =>
        sidePartials.collectFirst {
          case SidePartialRef(sideSpec, partialAttr)
              if sideSpec.originalExpr.semanticEquals(spec.originalExpr) =>
            val wrappedFinal = StarSchemaAggregateFunctionWrapper
              .wrapperFinal(spec.aggregate, partialAttr, spec.wrapperKey)
              .toAggregateExpression()
            if (expr.semanticEquals(spec.originalExpr)) {
              wrappedFinal
            } else {
              expr.transformUp {
                case ae: AggregateExpression if ae.semanticEquals(spec.originalExpr) =>
                  wrappedFinal
              }
            }
        }
    }
  }

  private def cleanAttr(attr: Attribute): AttributeReference = {
    AttributeReference(attr.name, attr.dataType, attr.nullable)(
      exprId = attr.exprId,
      qualifier = attr.qualifier)
  }

  private def hasPushableAggExpr(aggExprs: Seq[Expression]): Boolean = {
    aggExprs.exists {
      case Alias(expr, _) => isPushableExpr(expr)
      case expr => isPushableExpr(expr)
    }
  }

  private def isPushableExpr(expr: Expression): Boolean = {
    pushableSpec(expr).isDefined
  }

  private def pushableSpec(expr: Expression): Option[SidePartialSpec] = {
    val candidates = expr.collect {
      case ae: AggregateExpression if !ae.isDistinct && ae.filter.isEmpty =>
        ae.aggregateFunction match {
          case da: DeclarativeAggregate if !da.isInstanceOf[StarSchemaAggregateFunctionWrapper] =>
            Some((ae, da))
          case _ => None
        }
    }.flatten

    if (candidates.size == 1) {
      val (targetAe, da) = candidates.head
      val stableExprSql = targetAe.canonicalized.sql
      Some(SidePartialSpec(targetAe, da, Integer.toUnsignedString(stableExprSql.hashCode)))
    } else {
      None
    }
  }

  private def collectSidePartials(
      aggExprs: Seq[NamedExpression],
      sideOutputSet: AttributeSet): Seq[SidePartialSpec] = {
    aggExprs
      .flatMap {
        case Alias(expr, _) =>
          pushableSpec(expr)
            .filter(spec => canPushToSide(spec.aggregate, sideOutputSet))
            .toSeq
        case expr =>
          pushableSpec(expr)
            .filter(spec => canPushToSide(spec.aggregate, sideOutputSet))
            .toSeq
      }
      .foldLeft(Seq.empty[SidePartialSpec]) {
        case (acc, spec) if acc.exists(_.originalExpr.semanticEquals(spec.originalExpr)) => acc
        case (acc, spec) => acc :+ spec
      }
  }

  private def canPushToSide(agg: DeclarativeAggregate, sideOutputSet: AttributeSet): Boolean = {
    agg.children.forall(_.references.forall(sideOutputSet.contains))
  }

  private def dedupeAttrs(attrs: Seq[Attribute]): Seq[Attribute] = {
    attrs.foldLeft(Seq.empty[Attribute]) {
      case (acc, attr) if acc.exists(_.semanticEquals(attr)) => acc
      case (acc, attr) => acc :+ attr
    }
  }

  private def referencedAttrsInOrder(expr: Expression): Seq[Attribute] = {
    expr.collect { case attr: Attribute => attr }
  }

  private def aggregateBufferAttrsInOrder(expr: Expression): Seq[Attribute] = {
    expr.collect {
      case ae: AggregateExpression =>
        ae.aggregateFunction.inputAggBufferAttributes ++ ae.aggregateFunction.aggBufferAttributes
    }.flatten
  }

  sealed private trait JoinSide {
    def partialName: String
    def plan(join: Join): LogicalPlan
    def outputSet(join: Join): AttributeSet
    def replace(join: Join, newPlan: LogicalPlan): Join
  }

  private case object JoinLeft extends JoinSide {
    override val partialName: String = "left_partial"
    override def plan(join: Join): LogicalPlan = join.left
    override def outputSet(join: Join): AttributeSet = join.left.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(left = newPlan)
  }

  private case object JoinRight extends JoinSide {
    override val partialName: String = "right_partial"
    override def plan(join: Join): LogicalPlan = join.right
    override def outputSet(join: Join): AttributeSet = join.right.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(right = newPlan)
  }
}

case class PushStarSchemaPreAggregateBatch(spark: SparkSession) extends Rule[LogicalPlan] {
  private val decimalAvgRule = DecimalAggregates
  private val pushRule = PushStarSchemaPreAggregate(spark)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isEnabled) {
      return plan
    }
    val decimalAvgRewrittenPlan = decimalAvgRule(plan)
    val pushed = pushRule(decimalAvgRewrittenPlan)
    pushed
  }

  private def isEnabled: Boolean = {
    GlutenConfig.get.enableStarSchemaJoinAggregateRules
  }
}
