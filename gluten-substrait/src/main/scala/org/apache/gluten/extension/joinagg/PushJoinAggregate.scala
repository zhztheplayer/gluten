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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, EqualTo, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.DecimalAggregates
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

case class PushJoinAggregate(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PredicateHelper {

  private case class SidePartialSpec(
      originalExpr: Expression,
      aggregate: DeclarativeAggregate,
      wrapperKey: String)

  private case class SidePartialRef(spec: SidePartialSpec, attr: AttributeReference)

  private var successfulPushCount: Int = 0

  def resetSuccessfulPushCount(): Unit = {
    successfulPushCount = 0
  }

  def getSuccessfulPushCount: Int = successfulPushCount

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isEnabled) {
      return plan
    }

    // 1) Aggregate+Join => FinalWrapperAgg(PartialWrapperAgg(...Join...))
    val split = splitAggregateWithJoin(plan)
    // 2) Exhaustively push PartialWrapperAgg through join edges.
    val pushed = pushPartialWrapperAggregate(split)
    // 3) Return rewritten plan with pushed partial wrapper aggregates.
    pushed
  }

  private def isEnabled: Boolean = GlutenConfig.get.enableJoinAggregateRules

  private def splitAggregateWithJoin(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case agg @ Aggregate(_, aggExprs, _)
          if !containsWrapperAggregateInOutput(aggExprs) &&
            hasPushableAggExpr(aggExprs) &&
            !hasDistinctAggExpr(aggExprs) =>
        splitAggregate(agg).getOrElse(agg)
    }
  }

  private def splitAggregate(agg: Aggregate): Option[LogicalPlan] = {
    extractJoin(agg.child).flatMap {
      case (join, wrapperRequiredAttrs, rebuild) =>
        val pushableSpecs = collectPushableSpecs(agg.aggregateExpressions)
        if (pushableSpecs.isEmpty) {
          None
        } else {
          val groupingRefAttrs = dedupeAttrs(agg.groupingExpressions.flatMap(referencedAttrsInOrder))
          val joinRefAttrs = dedupeAttrs(join.condition.toSeq.flatMap(referencedAttrsInOrder))
          val nonPushableAggAttrs = dedupeAttrs(agg.aggregateExpressions.flatMap {
            case Alias(expr, _) if !isPushableExpr(expr) => referencedAttrsInOrder(expr)
            case expr if !isPushableExpr(expr) => referencedAttrsInOrder(expr)
            case _ => Nil
          })
          val requiredAbovePartial =
            dedupeAttrs(groupingRefAttrs ++ joinRefAttrs ++ nonPushableAggAttrs)
          // `wrapperRequiredAttrs` propagated from project/filter extraction can be broader than
          // what is semantically required above the partial aggregate (e.g. measure inputs like
          // `ss_sales_price`), which would incorrectly bloat the pushed grouping keys.
          val requiredWrapperAttrs = wrapperRequiredAttrs.filter {
            attr => requiredAbovePartial.exists(_.semanticEquals(attr))
          }
          val partialGroupingKeys = dedupeAttrs(requiredAbovePartial ++ requiredWrapperAttrs)
          if (partialGroupingKeys.isEmpty) {
            None
          } else {
            val partialAliases = pushableSpecs.zipWithIndex.map {
              case (spec, idx) =>
                Alias(
                  JoinAggregateFunctionWrapper
                    .wrapperPartial(spec.aggregate, spec.wrapperKey)
                    .toAggregateExpression(),
                  s"pushed_agg_func_$idx"
                )()
            }

            // Keep wrapper-input dependencies (e.g. CASE operands from dimension columns) when
            // rebuilding extracted Project/Filter nodes. These inputs must be preserved for
            // aggregate pre-project generation, but they must not be promoted into grouping keys.
            val partialInputAttrs = dedupeAttrs(pushableSpecs.flatMap {
              spec => spec.aggregate.children.flatMap(referencedAttrsInOrder)
            })
            val rebuildRequiredAttrs =
              dedupeAttrs(partialGroupingKeys ++ partialAliases.map(_.toAttribute) ++ partialInputAttrs)
            val rebuiltChild = rebuild(join, rebuildRequiredAttrs)
            val partialAgg = Aggregate(
              groupingExpressions = partialGroupingKeys,
              aggregateExpressions = partialGroupingKeys ++ partialAliases,
              child = rebuiltChild
            )

            val partialOutputAttrs =
              partialAgg.output.drop(partialGroupingKeys.size).take(partialAliases.size).map(cleanAttr)
            val partialRefs = pushableSpecs.zip(partialOutputAttrs).map {
              case (spec, attr) => SidePartialRef(spec, attr)
            }

            rewriteAggregateExpressions(agg.aggregateExpressions, partialRefs).map {
              rewrittenAggExprs =>
                agg.copy(
                  aggregateExpressions = rewrittenAggExprs,
                  child = partialAgg
                )
            }
          }
        }
    }
  }

  private def pushPartialWrapperAggregate(plan: LogicalPlan): LogicalPlan = {
    var current = plan
    var changed = true
    while (changed) {
      changed = false
      current = current.transformUp {
        case partialAgg @ Aggregate(groupingExprs, aggExprs, child)
            if isPurePartialWrapperAggregate(partialAgg) =>
          pushOnce(partialAgg, groupingExprs, aggExprs, child) match {
            case Some(newPlan) =>
              successfulPushCount += 1
              changed = true
              newPlan
            case None =>
              partialAgg
          }
      }
    }
    current
  }

  private def pushOnce(
      partialAgg: Aggregate,
      groupingExprs: Seq[Expression],
      aggExprs: Seq[NamedExpression],
      child: LogicalPlan): Option[LogicalPlan] = {
    extractJoin(child).flatMap {
      case (join, wrapperRequiredAttrs, rebuild) =>
        Seq(JoinLeft, JoinRight).iterator.flatMap {
          side =>
            pushPartialAggToJoinSide(
              partialAgg,
              join,
              groupingExprs,
              aggExprs,
              wrapperRequiredAttrs,
              rebuild,
              side)
        }.toSeq.headOption
    }
  }

  private def pushPartialAggToJoinSide(
      partialAgg: Aggregate,
      join: Join,
      groupingExprs: Seq[Expression],
      aggExprs: Seq[NamedExpression],
      wrapperRequiredAttrs: Seq[Attribute],
      rebuild: (Join, Seq[Attribute]) => LogicalPlan,
      side: JoinSide): Option[LogicalPlan] = {
    val wrapperAliases = collectPartialWrapperAliases(aggExprs)
    if (wrapperAliases.isEmpty) {
      return None
    }

    val sideOutputSet = side.outputSet(join)
    val allPushable = wrapperAliases.forall {
      case (_, wrapper) => canPushToSide(wrapper.innerAgg, sideOutputSet)
    }
    if (!allPushable) {
      return None
    }

    val sideGroupingAttrs = groupingExprs
      .flatMap(referencedAttrsInOrder)
      .collect { case a: Attribute if sideOutputSet.contains(a) => a }
    val sideJoinKeys = join.condition.toSeq.flatMap(splitConjunctivePredicates).collect {
      case EqualTo(l: Attribute, r: Attribute)
          if sideOutputSet.contains(l) && !sideOutputSet.contains(r) =>
        l
      case EqualTo(l: Attribute, r: Attribute)
          if sideOutputSet.contains(r) && !sideOutputSet.contains(l) =>
        r
    }
    val sideJoinCondAttrs = join.condition.toSeq
      .flatMap(referencedAttrsInOrder)
      .collect { case a: Attribute if sideOutputSet.contains(a) => a }

    val sideNonPushableAggAttrs = dedupeAttrs(aggExprs.flatMap {
      case Alias(expr, _) if !isPushableExpr(expr) && !containsWrapperAggregateExpr(expr) =>
        referencedAttrsInOrder(expr).collect {
          case a: Attribute if sideOutputSet.contains(a) => a
        }
      case expr if !isPushableExpr(expr) && !containsWrapperAggregateExpr(expr) =>
        referencedAttrsInOrder(expr).collect {
          case a: Attribute if sideOutputSet.contains(a) => a
        }
      case _ => Nil
    })
    val sideWrapperRequiredAttrs = dedupeAttrs(wrapperRequiredAttrs.collect {
      case a: Attribute if sideOutputSet.contains(a) => a
    })
    // Do not use project/filter propagated attrs to build pushed grouping keys.
    // They can include measure-only attrs (e.g. ss_sales_price) and over-constrain
    // pre-aggregation. However, extracted project dependencies are still required here to
    // preserve grouping semantics for derived grouping expressions such as
    // `substr(w_warehouse_name, 1, 20)`.
    val pushedGrouping =
      dedupeAttrs(
        sideGroupingAttrs ++
          sideJoinKeys ++
          sideJoinCondAttrs ++
          sideNonPushableAggAttrs ++
          sideWrapperRequiredAttrs)
    if (pushedGrouping.isEmpty) {
      return None
    }

    val pushedWrapperAliases = wrapperAliases.map {
      case (alias, wrapper) =>
        val wrapped = JoinAggregateFunctionWrapper
          .wrapperPartial(wrapper.innerAgg, wrapper.wrapperKey)
          .toAggregateExpression()
        Alias(wrapped, alias.name)(
          exprId = alias.exprId,
          qualifier = alias.qualifier,
          explicitMetadata = alias.explicitMetadata,
          nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
        )
    }

    val pushedAgg = Aggregate(
      groupingExpressions = pushedGrouping,
      aggregateExpressions = pushedGrouping ++ pushedWrapperAliases,
      child = side.plan(join)
    )

    val pushedJoin = side.replace(join, pushedAgg)
    val requiredAttrs = partialAgg.output.collect { case a: Attribute => a }
    Some(rebuild(pushedJoin, requiredAttrs))
  }

  private def isPurePartialWrapperAggregate(agg: Aggregate): Boolean = {
    val wrapperAliases = collectPartialWrapperAliases(agg.aggregateExpressions)
    wrapperAliases.nonEmpty && agg.aggregateExpressions.forall {
      case Alias(_: AggregateExpression, _) => true
      case _: AggregateExpression => false
      case _ => true
    }
  }

  private def collectPartialWrapperAliases(
      output: Seq[NamedExpression]): Seq[(Alias, JoinAggregateFunctionWrapper)] = {
    output.collect {
      case alias @ Alias(AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _), _)
          if wrapper.targetPhase == JoinAggregateFunctionWrapper.PartialPhase =>
        (alias, wrapper)
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
                val requiredAttrSet = AttributeSet(requiredAttrs)
                val retainedProjectList = project.projectList.filter {
                  ne => requiredAttrSet.contains(ne.toAttribute)
                }
                // If a retained project expression depends on child columns (e.g. CASE refs),
                // propagate those dependencies when rebuilding the join subtree.
                val requiredForChild =
                  dedupeAttrs(requiredAttrs ++ retainedProjectList.flatMap(referencedAttrsInOrder))
                val rebuiltChild = rebuild(j, requiredForChild)
                val projectOutputSet = AttributeSet(project.projectList.map(_.toAttribute))
                val passThroughAttrs = requiredAttrs.filter {
                  attr => rebuiltChild.outputSet.contains(attr) && !projectOutputSet.contains(attr)
                }
                // Only keep project outputs required by the consumer above this extracted join.
                // This allows pushdown to replace measure columns with partial-wrapper outputs.
                project.copy(
                  projectList = retainedProjectList ++ dedupeAttrs(passThroughAttrs),
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

  private def cleanAttr(attr: Attribute): AttributeReference = {
    AttributeReference(attr.name, attr.dataType, attr.nullable)(
      exprId = attr.exprId,
      qualifier = attr.qualifier)
  }

  private def containsWrapperAggregateInOutput(aggExprs: Seq[NamedExpression]): Boolean = {
    aggExprs.exists {
      _.exists {
        case AggregateExpression(_: JoinAggregateFunctionWrapper, _, _, _, _) => true
        case _ => false
      }
    }
  }

  private def containsWrapperAggregateExpr(expr: Expression): Boolean = {
    expr.exists {
      case AggregateExpression(_: JoinAggregateFunctionWrapper, _, _, _, _) => true
      case _ => false
    }
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

  private def hasDistinctAggExpr(aggExprs: Seq[NamedExpression]): Boolean = {
    aggExprs.exists {
      _.exists {
        case ae: AggregateExpression if ae.isDistinct => true
        case _ => false
      }
    }
  }

  private def pushableSpec(expr: Expression): Option[SidePartialSpec] = {
    val candidates = expr.collect {
      case ae: AggregateExpression if !ae.isDistinct && ae.filter.isEmpty =>
        ae.aggregateFunction match {
          case da: DeclarativeAggregate if !da.isInstanceOf[JoinAggregateFunctionWrapper] =>
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

  private def collectPushableSpecs(aggExprs: Seq[NamedExpression]): Seq[SidePartialSpec] = {
    aggExprs
      .flatMap {
        case Alias(expr, _) => pushableSpec(expr).toSeq
        case expr => pushableSpec(expr).toSeq
      }
      .foldLeft(Seq.empty[SidePartialSpec]) {
        case (acc, spec) if acc.exists(_.originalExpr.semanticEquals(spec.originalExpr)) => acc
        case (acc, spec) => acc :+ spec
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
            val wrappedFinal = JoinAggregateFunctionWrapper
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

  sealed private trait JoinSide {
    def plan(join: Join): LogicalPlan
    def outputSet(join: Join): AttributeSet
    def replace(join: Join, newPlan: LogicalPlan): Join
  }

  private case object JoinLeft extends JoinSide {
    override def plan(join: Join): LogicalPlan = join.left
    override def outputSet(join: Join): AttributeSet = join.left.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(left = newPlan)
  }

  private case object JoinRight extends JoinSide {
    override def plan(join: Join): LogicalPlan = join.right
    override def outputSet(join: Join): AttributeSet = join.right.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(right = newPlan)
  }
}

case class PushJoinAggregateBatch(spark: SparkSession) extends Rule[LogicalPlan] {
  private val decimalAvgRule = DecimalAggregates
  private val pushRule = PushJoinAggregate(spark)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isEnabled) {
      return plan
    }
    val decimalAvgRewrittenPlan = decimalAvgRule(plan)
    pushRule(decimalAvgRewrittenPlan)
  }

  private def isEnabled: Boolean = {
    GlutenConfig.get.enableJoinAggregateRules
  }
}
