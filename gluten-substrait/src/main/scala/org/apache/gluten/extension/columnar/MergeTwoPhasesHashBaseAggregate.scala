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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}

/**
 * Merge two phase hash-based aggregate into one aggregate in the spark plan if there is no shuffle:
 *
 * Merge HashAggregate(t1.i, SUM, final) + HashAggregate(t1.i, SUM, partial) into
 * HashAggregate(t1.i, SUM, complete)
 *
 * Note: this rule must be applied before the `PullOutPreProject` rule, because the
 * `PullOutPreProject` rule will modify the attributes in some cases.
 */
case class MergeTwoPhasesHashBaseAggregate(session: SparkSession)
  extends Rule[SparkPlan]
  with Logging {

  val glutenConf: GlutenConfig = GlutenConfig.get
  val scanOnly: Boolean = glutenConf.enableScanOnly
  val enableColumnarHashAgg: Boolean = !scanOnly && glutenConf.enableColumnarHashAgg
  val replaceSortAggWithHashAgg: Boolean = GlutenConfig.get.forceToUseHashAgg
  val mergeTwoPhasesAggEnabled: Boolean = GlutenConfig.get.mergeTwoPhasesAggEnabled

  private def mergedMode(childMode: AggregateMode, parentMode: AggregateMode): Option[AggregateMode] = {
    (childMode, parentMode) match {
      // Existing merge: regular Spark 2-stage aggregate.
      case (Partial, Final) => Some(Complete)
      // Join-aggregate unwrap path:
      // A(Partial) + B(PartialMerge) can be fused into one Partial aggregate.
      // This preserves semantics and avoids keeping an extra aggregate stage.
      case (Partial, PartialMerge) => Some(Partial)
      // Join-aggregate unwrap path:
      // C(PartialMerge) + D(Final) can be fused into one Final aggregate.
      case (PartialMerge, Final) => Some(Final)
      case _ => None
    }
  }

  private def mergeModes(
      childAgg: BaseAggregateExec,
      parentAgg: BaseAggregateExec): Option[Seq[AggregateMode]] = {
    val zipped = childAgg.aggregateExpressions.zip(parentAgg.aggregateExpressions)
    val merged = zipped.map {
      case (childExpr, parentExpr) => mergedMode(childExpr.mode, parentExpr.mode)
    }
    if (merged.forall(_.isDefined)) Some(merged.flatten) else None
  }

  private def canMergeTwoPhases(childAgg: BaseAggregateExec, parentAgg: BaseAggregateExec): Boolean = {
    // Keep this conservative:
    // - no aggregate FILTER clauses
    // - every aggregate-expression pair must have a valid mode mapping
    // - logicalLink must match to guarantee same logical aggregate origin
    if (
      childAgg.aggregateExpressions.forall(_.filter.isEmpty) &&
      parentAgg.aggregateExpressions.forall(_.filter.isEmpty) &&
      mergeModes(childAgg, parentAgg).isDefined
    ) {
      (parentAgg.logicalLink, childAgg.logicalLink) match {
        case (Some(agg1), Some(agg2)) => agg1.sameResult(agg2)
        case _ => false
      }
    } else {
      false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!mergeTwoPhasesAggEnabled || !enableColumnarHashAgg) {
      plan
    } else {
      plan.transformDown {
        case hashAgg @ HashAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: HashAggregateExec) if !isStreaming && canMergeTwoPhases(child, hashAgg) =>
          val mergedModes = mergeModes(child, hashAgg).get
          val mergedAggregateExpressions =
            aggregateExpressions.zip(mergedModes).map {
              case (ae, mode) => ae.copy(mode = mode)
            }
          hashAgg.copy(
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = mergedAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        // PullOutPreProject and wrapper-unwrapping can leave a pass-through projection
        // between two mergeable hash aggregates. Rebind parent expressions through the
        // project aliases and then merge phases as usual.
        case hashAgg @ HashAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              project @ ProjectExec(projectList, child: HashAggregateExec))
            if !isStreaming && isAliasOnlyProject(projectList) && canMergeTwoPhases(child, hashAgg) =>
          val projectAttrMap = buildProjectAttrMap(project)
          val reboundGrouping = hashAgg.groupingExpressions.map(rewriteByProjectMap(_, projectAttrMap))
          val reboundAggExprs = hashAgg.aggregateExpressions.map {
            ae => rewriteByProjectMap(ae, projectAttrMap).asInstanceOf[AggregateExpression]
          }
          val reboundResultExprs = hashAgg.resultExpressions.map {
            re => rewriteByProjectMap(re, projectAttrMap).asInstanceOf[NamedExpression]
          }
          val reboundRequiredDistribution = hashAgg.requiredChildDistributionExpressions.map {
            _.map(rewriteByProjectMap(_, projectAttrMap))
          }
          val mergedModes = mergeModes(child, hashAgg).get
          val mergedAggregateExpressions =
            reboundAggExprs.zip(mergedModes).map {
              case (ae, mode) => ae.copy(mode = mode)
            }
          hashAgg.copy(
            requiredChildDistributionExpressions = reboundRequiredDistribution,
            groupingExpressions = reboundGrouping,
            aggregateExpressions = mergedAggregateExpressions,
            initialInputBufferOffset = 0,
            resultExpressions = reboundResultExprs,
            child = child.child
          )
        case objectHashAgg @ ObjectHashAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: ObjectHashAggregateExec)
            if !isStreaming && canMergeTwoPhases(child, objectHashAgg) =>
          val mergedModes = mergeModes(child, objectHashAgg).get
          val mergedAggregateExpressions =
            aggregateExpressions.zip(mergedModes).map {
              case (ae, mode) => ae.copy(mode = mode)
            }
          objectHashAgg.copy(
            requiredChildDistributionExpressions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = mergedAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        case sortAgg @ SortAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: SortAggregateExec)
            if replaceSortAggWithHashAgg && !isStreaming && canMergeTwoPhases(child, sortAgg) =>
          val mergedModes = mergeModes(child, sortAgg).get
          val mergedAggregateExpressions =
            aggregateExpressions.zip(mergedModes).map {
              case (ae, mode) => ae.copy(mode = mode)
            }
          sortAgg.copy(
            requiredChildDistributionExpressions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = mergedAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        case plan: SparkPlan => plan
      }
    }
  }

  private def isAliasOnlyProject(projectList: Seq[NamedExpression]): Boolean = {
    projectList.forall {
      case _: Attribute => true
      case Alias(_: Attribute, _) => true
      case _ => false
    }
  }

  private def buildProjectAttrMap(project: ProjectExec): Map[ExprId, Attribute] = {
    project.projectList.flatMap {
      case a: Attribute =>
        Some(a.exprId -> a)
      case Alias(a: Attribute, _) =>
        // Parent expressions bind by the alias output ExprId.
        Some(a.toAttribute.exprId -> a)
      case _ =>
        None
    }.toMap
  }

  private def rewriteByProjectMap[T <: Expression](
      expr: T,
      projectAttrMap: Map[ExprId, Attribute]): T = {
    expr.transformUp {
      case a: Attribute => projectAttrMap.getOrElse(a.exprId, a)
    }.asInstanceOf[T]
  }

}
