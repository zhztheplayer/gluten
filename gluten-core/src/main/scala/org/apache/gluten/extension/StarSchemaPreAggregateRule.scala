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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Cast, Divide, EqualTo, Expression, If, IsNotNull, Literal, Multiply, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, DoubleType, NumericType}

case class StarSchemaPreAggregateRule(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PredicateHelper {
  private case class SidePartialSpec(
      key: String,
      childExpr: Expression,
      aggregateBuilder: Expression => AggregateExpression,
      aliasBuilder: (String, Int) => String)

  private case class SidePartialRef(spec: SidePartialSpec, attr: AttributeReference)

  private case class PreAggInfo(
      plan: Aggregate,
      sideCnt: AttributeReference,
      sidePartials: Seq[SidePartialRef])

  private val expandRules: Seq[AggregateExpandRule] = Seq(
    CountOneExpandRule,
    SumExpandRule,
    AvgExpandRule
  )

  private var successfulPushCount: Int = 0

  def resetSuccessfulPushCount(): Unit = {
    successfulPushCount = 0
  }

  def getSuccessfulPushCount: Int = successfulPushCount

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case agg @ Aggregate(groupingExprs, aggExprs, child) if hasPushableAggExpr(aggExprs) =>
      extractJoin(child)
        .flatMap {
          case (join, wrapperRequiredAttrs, rebuild) =>
            Seq(JoinLeft, JoinRight).iterator
              .flatMap {
                side =>
                  buildPreAgg(join, groupingExprs, wrapperRequiredAttrs, aggExprs, side).flatMap {
                    preAggInfo =>
                      rewriteAggregateExpressions(
                        aggExprs,
                        preAggInfo.sideCnt,
                        side.outputSet(join),
                        preAggInfo.sidePartials).map {
                        rewrittenAggExprs =>
                          val rewrittenJoin = side.replace(join, preAggInfo.plan)
                          val requiredAttrs = dedupeAttrs(
                            (groupingExprs ++ rewrittenAggExprs).flatMap(referencedAttrsInOrder))
                          successfulPushCount += 1
                          agg.copy(
                            aggregateExpressions = rewrittenAggExprs,
                            child = rebuild(rewrittenJoin, requiredAttrs))
                      }
                  }
              }
              .toSeq
              .headOption
        }
        .getOrElse(agg)
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
                  child = rebuiltChild)
              })
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
    if (side.plan(join).output.exists(_.name == side.multiplierName)) {
      return None
    }

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
    val groupingKeys = dedupeAttrs(sideGroupingAttrs ++ wrapperAttrs ++ joinKeys)
    if (groupingKeys.isEmpty) {
      None
    } else {
      val sidePartials = collectSidePartials(aggExprs, sideOutputSet)
      val sidePartialAliases = sidePartials.zipWithIndex.map {
        case (spec, idx) =>
          Alias(
            spec.aggregateBuilder(spec.childExpr),
            spec.aliasBuilder(side.multiplierName, idx))()
      }

      val preAgg = Aggregate(
        groupingExpressions = groupingKeys,
        aggregateExpressions = groupingKeys ++ Seq(
          Alias(Count(Seq(Literal(1))).toAggregateExpression(), side.multiplierName)()) ++
          sidePartialAliases,
        child = side.plan(join)
      )
      val sideCnt = cleanAttr(preAgg.output.find(_.name == side.multiplierName).get)
      val sidePartialOutputAttrs =
        preAgg.output.drop(groupingKeys.size + 1).take(sidePartials.size).map(cleanAttr)
      val sidePartialRefs = sidePartials.zip(sidePartialOutputAttrs).map {
        case (spec, attr) => SidePartialRef(spec, attr)
      }
      Some(PreAggInfo(preAgg, sideCnt, sidePartialRefs))
    }
  }

  private def rewriteAggregateExpressions(
      aggExprs: Seq[NamedExpression],
      sideCnt: Attribute,
      sideOutputSet: org.apache.spark.sql.catalyst.expressions.AttributeSet,
      sidePartials: Seq[SidePartialRef]): Option[Seq[NamedExpression]] = {
    val cleanSideCnt = cleanAttr(sideCnt)
    var rewrittenAny = false

    val rewrittenAggExprs = aggExprs.map {
      case alias @ Alias(expr, _) if isExpandableAggExpr(expr) =>
        rewriteAggregateExpr(expr, cleanSideCnt, sideOutputSet, sidePartials) match {
          case Some(rewrittenExpr) =>
            rewrittenAny = true
            Alias(rewrittenExpr, alias.name)(
              exprId = alias.exprId,
              qualifier = alias.qualifier,
              explicitMetadata = alias.explicitMetadata,
              nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys)
          case None =>
            return None
        }

      case alias @ Alias(_, _) =>
        alias

      case other if isExpandableAggExpr(other) =>
        return None

      case other =>
        other
    }

    if (rewrittenAny) Some(rewrittenAggExprs) else None
  }

  private def rewriteAggregateExpr(
      expr: Expression,
      sideCnt: AttributeReference,
      sideOutputSet: org.apache.spark.sql.catalyst.expressions.AttributeSet,
      sidePartials: Seq[SidePartialRef]): Option[Expression] = {
    expandRules.iterator
      .flatMap(_.rewrite(expr, sideCnt, sideOutputSet, sidePartials))
      .toSeq
      .headOption
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
      case Alias(ae: AggregateExpression, _) => isExpandableAggExpr(ae)
      case ae: AggregateExpression => isExpandableAggExpr(ae)
      case _ => false
    }
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

  private def isExpandableAggExpr(expr: Expression): Boolean = {
    expandRules.exists(_.isSupported(expr))
  }

  sealed private trait AggregateExpandRule {
    def isSupported(expr: Expression): Boolean
    def sidePartials(expr: Expression, sideOutputSet: AttributeSet): Seq[SidePartialSpec] = Nil
    def rewrite(
        expr: Expression,
        sideCnt: AttributeReference,
        sideOutputSet: org.apache.spark.sql.catalyst.expressions.AttributeSet,
        sidePartials: Seq[SidePartialRef]): Option[Expression]
  }

  private object CountOneExpandRule extends AggregateExpandRule {
    override def isSupported(expr: Expression): Boolean = isCountOne(expr)

    override def rewrite(
        expr: Expression,
        sideCnt: AttributeReference,
        sideOutputSet: org.apache.spark.sql.catalyst.expressions.AttributeSet,
        sidePartials: Seq[SidePartialRef]): Option[Expression] = {
      if (isCountOne(expr)) {
        Some(Sum(sideCnt).toAggregateExpression())
      } else {
        None
      }
    }

    private def isCountOne(expr: Expression): Boolean = {
      expr match {
        case ae: AggregateExpression =>
          ae.aggregateFunction match {
            case Count(Seq(Literal(1, _))) => true
            case _ => false
          }
        case _ => false
      }
    }
  }

  private object SumExpandRule extends AggregateExpandRule {
    override def isSupported(expr: Expression): Boolean = sumChild(expr).isDefined

    override def sidePartials(
        expr: Expression,
        sideOutputSet: AttributeSet): Seq[SidePartialSpec] = {
      sumChild(expr)
        .flatMap(sidePartialCandidate(_, sideOutputSet))
        .map {
          candidate =>
            SidePartialSpec(
              key = "sum",
              childExpr = candidate,
              aggregateBuilder = child => Sum(child).toAggregateExpression(),
              aliasBuilder = (sideName, idx) => s"${sideName}_sum_$idx")
        }
        .toSeq
    }

    override def rewrite(
        expr: Expression,
        sideCnt: AttributeReference,
        sideOutputSet: org.apache.spark.sql.catalyst.expressions.AttributeSet,
        sidePartials: Seq[SidePartialRef]): Option[Expression] = {
      sumChild(expr)
        .flatMap {
          childExpr =>
            rewriteWithSidePartial(childExpr, sideCnt, sideOutputSet, sidePartials)
              .orElse {
                if (canPushToSide(childExpr, sideOutputSet)) {
                  multiplyBySideCount(childExpr, sideCnt)
                } else {
                  None
                }
              }
        }
        .map {
          weightedChild => castIfNeeded(Sum(weightedChild).toAggregateExpression(), expr.dataType)
        }
    }

    private def rewriteWithSidePartial(
        childExpr: Expression,
        sideCnt: AttributeReference,
        sideOutputSet: AttributeSet,
        sidePartials: Seq[SidePartialRef]): Option[Expression] = {
      sidePartials.iterator
        .flatMap {
          case SidePartialRef(spec, candidateAttr) if spec.key == "sum" =>
            val candidateExpr = spec.childExpr
            if (childExpr.semanticEquals(candidateExpr)) {
              if (candidateExpr.references.forall(attr => !sideOutputSet.contains(attr))) {
                multiplyBySideCount(
                  candidateAttr,
                  sideCnt,
                  castMultiplierTo = Some(candidateAttr.dataType))
              } else {
                None
              }
            } else {
              extractSideFactor(childExpr, sideOutputSet, candidateExpr)
                .map(nonSideExpr => Multiply(candidateAttr, nonSideExpr))
            }
          case _ =>
            None
        }
        .toSeq
        .headOption
    }

    private def extractSideFactor(
        childExpr: Expression,
        sideOutputSet: AttributeSet,
        candidateExpr: Expression): Option[Expression] = {
      childExpr match {
        case multiply: Multiply if multiply.left.semanticEquals(candidateExpr) =>
          val right = multiply.right
          if (right.references.forall(attr => !sideOutputSet.contains(attr))) {
            Some(right)
          } else {
            None
          }
        case multiply: Multiply if multiply.right.semanticEquals(candidateExpr) =>
          val left = multiply.left
          if (left.references.forall(attr => !sideOutputSet.contains(attr))) {
            Some(left)
          } else {
            None
          }
        case _ =>
          None
      }
    }

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

    private def sidePartialCandidate(
        childExpr: Expression,
        sideOutputSet: AttributeSet): Option[Expression] = {
      val refs = childExpr.references
      if (
        refs.nonEmpty &&
        refs.forall(sideOutputSet.contains) &&
        !refs.exists(isRuleGeneratedSideAttr(_, sideOutputSet))
      ) {
        Some(childExpr)
      } else {
        childExpr match {
          case multiply: Multiply
              if multiply.left.references.nonEmpty &&
                multiply.left.references.forall(sideOutputSet.contains) &&
                !multiply.left.references.exists(isRuleGeneratedSideAttr(_, sideOutputSet)) &&
                multiply.right.references.forall(attr => !sideOutputSet.contains(attr)) =>
            Some(multiply.left)
          case multiply: Multiply
              if multiply.right.references.nonEmpty &&
                multiply.right.references.forall(sideOutputSet.contains) &&
                !multiply.right.references.exists(isRuleGeneratedSideAttr(_, sideOutputSet)) &&
                multiply.left.references.forall(attr => !sideOutputSet.contains(attr)) =>
            Some(multiply.right)
          case _ =>
            None
        }
      }
    }

    private def isRuleGeneratedSideAttr(attr: Attribute, sideOutputSet: AttributeSet): Boolean = {
      sideOutputSet.contains(attr) &&
      (attr.name.startsWith("left_mult") || attr.name.startsWith("right_mult"))
    }
  }

  private object AvgExpandRule extends AggregateExpandRule {
    override def isSupported(expr: Expression): Boolean = avgChild(expr).isDefined

    override def rewrite(
        expr: Expression,
        sideCnt: AttributeReference,
        sideOutputSet: org.apache.spark.sql.catalyst.expressions.AttributeSet,
        sidePartials: Seq[SidePartialRef]): Option[Expression] = {
      avgChild(expr).flatMap {
        childExpr =>
          if (canPushToSide(childExpr, sideOutputSet)) {
            multiplyBySideCount(
              Cast(childExpr, DoubleType),
              sideCnt,
              castMultiplierTo = Some(DoubleType)).map {
              weightedExpr =>
                val weightedSum = Sum(weightedExpr).toAggregateExpression()
                val weightedCount =
                  Sum(If(IsNotNull(childExpr), Cast(sideCnt, DoubleType), Literal(0.0)))
                    .toAggregateExpression()
                castIfNeeded(Divide(weightedSum, weightedCount), expr.dataType)
            }
          } else {
            None
          }
      }
    }

    private def avgChild(expr: Expression): Option[Expression] = {
      expr match {
        case ae: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression =>
          ae.aggregateFunction match {
            case Average(child, _) => Some(child)
            case _ => None
          }
        case _ => None
      }
    }
  }

  private def collectSidePartials(
      aggExprs: Seq[NamedExpression],
      sideOutputSet: AttributeSet): Seq[SidePartialSpec] = {
    aggExprs
      .flatMap {
        case Alias(expr, _) =>
          expandRules.flatMap(_.sidePartials(expr, sideOutputSet))
        case expr =>
          expandRules.flatMap(_.sidePartials(expr, sideOutputSet))
      }
      .foldLeft(Seq.empty[SidePartialSpec]) {
        case (acc, spec)
            if acc.exists(
              existing =>
                existing.key == spec.key && existing.childExpr.semanticEquals(spec.childExpr)) =>
          acc
        case (acc, spec) =>
          acc :+ spec
      }
  }

  sealed private trait JoinSide {
    def multiplierName: String
    def plan(join: Join): LogicalPlan
    def outputSet(join: Join): org.apache.spark.sql.catalyst.expressions.AttributeSet
    def replace(join: Join, newPlan: LogicalPlan): Join
  }

  private case object JoinLeft extends JoinSide {
    override val multiplierName: String = "left_mult"
    override def plan(join: Join): LogicalPlan = join.left
    override def outputSet(join: Join): org.apache.spark.sql.catalyst.expressions.AttributeSet =
      join.left.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(left = newPlan)
  }

  private case object JoinRight extends JoinSide {
    override val multiplierName: String = "right_mult"
    override def plan(join: Join): LogicalPlan = join.right
    override def outputSet(join: Join): org.apache.spark.sql.catalyst.expressions.AttributeSet =
      join.right.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(right = newPlan)
  }

  private def multiplyBySideCount(
      valueExpr: Expression,
      sideCnt: AttributeReference,
      castMultiplierTo: Option[DataType] = None): Option[Expression] = {
    val valueType = valueExpr.dataType
    valueType match {
      case _: NumericType =>
        val multiplierType = castMultiplierTo.getOrElse(valueType)
        Some(Multiply(valueExpr, Cast(sideCnt, multiplierType)))
      case _ => None
    }
  }

  private def castIfNeeded(expr: Expression, targetType: DataType): Expression = {
    if (expr.dataType == targetType) expr else Cast(expr, targetType)
  }
}
