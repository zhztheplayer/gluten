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
package io.glutenproject.extension.columnar

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution._
import io.glutenproject.extension.{GlutenPlan, ValidationResult}
import io.glutenproject.extension.columnar.FallbackHints.EncodeTransformableTagImplicits
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.utils.PhysicalPlanSelector

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.StringType

import org.apache.commons.lang3.exception.ExceptionUtils

sealed trait FallbackHint {
  val stacktrace: Option[String] =
    if (FallbackHints.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None
}

case class FALLBACK(reason: Option[String], appendReasonIfExists: Boolean = true)
  extends FallbackHint

object FallbackHints {
  val TAG: TreeNodeTag[FallbackHint] =
    TreeNodeTag[FallbackHint]("io.glutenproject.fallback")

  val DEBUG = false

  def isTaggedFallback(plan: SparkPlan): Boolean = {
    getHintOption(plan) match {
      case Some(FALLBACK(_, _)) => true
      case _ => false
    }
  }

  /**
   * NOTE: To be deprecated. Do not create new usages of this method.
   *
   * Since it's usually not safe to consider a plan "transformable" during validation phase. Another
   * validation rule could turn "transformable" to "fallback" before implementing the plan within
   * Gluten transformers. Thus the returned value of this method could not be always reliable.
   */
  def isNotTaggedFallback(plan: SparkPlan): Boolean = {
    getHintOption(plan) match {
      case None => true
      case _ => false
    }
  }

  def tag(plan: SparkPlan, hint: FallbackHint): Unit = {
    val mergedHint = getHintOption(plan)
      .map {
        case originalHint @ FALLBACK(Some(originalReason), originAppend) =>
          hint match {
            case FALLBACK(Some(newReason), append) =>
              if (originAppend && append) {
                FALLBACK(Some(originalReason + "; " + newReason))
              } else if (originAppend) {
                FALLBACK(Some(originalReason))
              } else if (append) {
                FALLBACK(Some(newReason))
              } else {
                FALLBACK(Some(originalReason), false)
              }
            case FALLBACK(None, _) =>
              originalHint
            case _ =>
              throw new UnsupportedOperationException(
                "Plan was already tagged as non-transformable, " +
                  s"cannot mark it as transformable after that:\n${plan.toString()}")
          }
        case _ =>
          hint
      }
      .getOrElse(hint)
    plan.setTagValue(TAG, mergedHint)
  }

  def untag(plan: SparkPlan): Unit = {
    plan.unsetTagValue(TAG)
  }

  def tagFallback(plan: SparkPlan, validationResult: ValidationResult): Unit = {
    if (!validationResult.isValid) {
      tag(plan, FALLBACK(validationResult.reason))
    }
  }

  def tagFallback(plan: SparkPlan, reason: String): Unit = {
    tag(plan, FALLBACK(Some(reason)))
  }

  def tagFallbackRecursively(plan: SparkPlan, hint: FALLBACK): Unit = {
    plan.foreach {
      case _: GlutenPlan => // ignore
      case other => tag(other, hint)
    }
  }

  def getHint(plan: SparkPlan): FallbackHint = {
    getHintOption(plan).getOrElse(
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.toString()))
  }

  private def getHintOption(plan: SparkPlan): Option[FallbackHint] = {
    plan.getTagValue(TAG)
  }

  implicit class EncodeTransformableTagImplicits(validationResult: ValidationResult) {
    def tagOnFallback(plan: SparkPlan): Unit = {
      if (validationResult.isValid) {
        return
      }
      val newTag = FALLBACK(validationResult.reason)
      tag(plan, newTag)
    }
  }
}

case class FallbackOnANSIMode(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {
    if (GlutenConfig.getConf.enableAnsiMode) {
      plan.foreach(FallbackHints.tagFallback(_, "does not support ansi mode"))
    }
    plan
  }
}

case class FallbackMultiCodegens(session: SparkSession) extends Rule[SparkPlan] {
  lazy val columnarConf: GlutenConfig = GlutenConfig.getConf
  lazy val physicalJoinOptimize = columnarConf.enablePhysicalJoinOptimize
  lazy val optimizeLevel: Integer = columnarConf.physicalJoinOptimizationThrottle

  def existsMultiCodegens(plan: SparkPlan, count: Int = 0): Boolean =
    plan match {
      case plan: CodegenSupport if plan.supportCodegen =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.exists(existsMultiCodegens(_, count + 1))
      case plan: ShuffledHashJoinExec =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.exists(existsMultiCodegens(_, count + 1))
      case other => false
    }

  def tagFallback(plan: SparkPlan): SparkPlan = {
    FallbackHints.tagFallback(plan, "fallback multi codegens")
    plan
  }

  def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport =>
      plan.supportCodegen
    case _ => false
  }

  def isAQEShuffleReadExec(plan: SparkPlan): Boolean = {
    plan match {
      case _: AQEShuffleReadExec => true
      case _ => false
    }
  }

  def tagFallbackRecursively(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        tagFallback(p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens)))
      case p: BroadcastExchangeExec =>
        tagFallback(p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens)))
      case p: ShuffledHashJoinExec =>
        tagFallback(p.withNewChildren(p.children.map(tagFallbackRecursively)))
      case p if !supportCodegen(p) =>
        // insert row guard them recursively
        p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens))
      case p if isAQEShuffleReadExec(p) =>
        p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens))
      case p: QueryStageExec => p
      case p => tagFallback(p.withNewChildren(p.children.map(tagFallbackRecursively)))
    }
  }

  def tagNotTransformableForMultiCodegens(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan if existsMultiCodegens(plan) =>
        tagFallbackRecursively(plan)
      case other =>
        other.withNewChildren(other.children.map(tagNotTransformableForMultiCodegens))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {
    if (physicalJoinOptimize) {
      tagNotTransformableForMultiCodegens(plan)
    } else plan
  }
}

/**
 * This rule plans [[RDDScanExec]] with a fake schema to make gluten work, because gluten does not
 * support empty output relation, see [[FallbackEmptySchemaRelation]].
 */
case class PlanOneRowRelation(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.getConf.enableOneRowRelationColumnar) {
      return plan
    }

    plan.transform {
      // We should make sure the output does not change, e.g.
      // Window
      //   OneRowRelation
      case u: UnaryExecNode
          if u.child.isInstanceOf[RDDScanExec] &&
            u.child.asInstanceOf[RDDScanExec].name == "OneRowRelation" &&
            u.outputSet != u.child.outputSet =>
        val rdd = spark.sparkContext.parallelize(InternalRow(null) :: Nil, 1)
        val attr = AttributeReference("fake_column", StringType)()
        u.withNewChildren(RDDScanExec(attr :: Nil, rdd, "OneRowRelation") :: Nil)
    }
  }
}

/**
 * FIXME To be removed: Since Velox backend is the only one to use the strategy, and we already
 * support offloading zero-column batch in ColumnarBatchInIterator via PR #3309.
 *
 * We'd make sure all Velox operators be able to handle zero-column input correctly then remove the
 * rule together with [[PlanOneRowRelation]].
 */
case class FallbackEmptySchemaRelation() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case p =>
      if (BackendsApiManager.getSettings.fallbackOnEmptySchema(p)) {
        if (p.children.exists(_.output.isEmpty)) {
          // Some backends are not eligible to offload plan with zero-column input.
          // If any child have empty output, mark the plan and that child as UNSUPPORTED.
          FallbackHints.tagFallback(p, "at least one of its children has empty output")
          p.children.foreach {
            child =>
              if (child.output.isEmpty && !child.isInstanceOf[WriteFilesExec]) {
                FallbackHints.tagFallback(child, "at least one of its children has empty output")
              }
          }
        }
      }
      p
  }
}

/**
 * Velox BloomFilter's implementation is different from Spark's. So if might_contain falls back, we
 * need fall back related bloom filter agg.
 */
case class FallbackBloomFilterAggIfNeeded() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan =
    if (
      GlutenConfig.getConf.enableNativeBloomFilter &&
      BackendsApiManager.getSettings.enableBloomFilterAggFallbackRule()
    ) {
      plan.transformDown {
        case p if FallbackHints.isTaggedFallback(p) =>
          handleBloomFilterFallback(p)
          p
      }
    } else {
      plan
    }

  object SubPlanFromBloomFilterMightContain {
    def unapply(expr: Expression): Option[SparkPlan] =
      SparkShimLoader.getSparkShims.extractSubPlanFromMightContain(expr)
  }

  private def handleBloomFilterFallback(plan: SparkPlan): Unit = {
    def tagFallbackRecursively(p: SparkPlan): Unit = {
      p match {
        case agg: org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
            if SparkShimLoader.getSparkShims.hasBloomFilterAggregate(agg) =>
          FallbackHints.tagFallback(agg, "related BloomFilterMightContain falls back")
          tagFallbackRecursively(agg.child)
        case a: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec =>
          tagFallbackRecursively(a.executedPlan)
        case _ =>
          p.children.map(tagFallbackRecursively)
      }
    }

    plan.transformExpressions {
      case expr @ SubPlanFromBloomFilterMightContain(p: SparkPlan) =>
        tagFallbackRecursively(p)
        expr
    }
  }
}

// This rule will try to convert a plan into plan transformer.
// The doValidate function will be called to check if the conversion is supported.
// If false is returned or any unsupported exception is thrown, a row guard will
// be added on the top of that plan to prevent actual conversion.
case class AddFallbackHintRule() extends Rule[SparkPlan] {
  val columnarConf: GlutenConfig = GlutenConfig.getConf
  val scanOnly: Boolean = columnarConf.enableScanOnly
  val enableColumnarShuffle: Boolean =
    !scanOnly && BackendsApiManager.getSettings.supportColumnarShuffleExec()
  val enableColumnarSort: Boolean = !scanOnly && columnarConf.enableColumnarSort
  val enableColumnarWindow: Boolean = !scanOnly && columnarConf.enableColumnarWindow
  val enableColumnarSortMergeJoin: Boolean = !scanOnly &&
    BackendsApiManager.getSettings.supportSortMergeJoinExec()
  val enableColumnarBatchScan: Boolean = columnarConf.enableColumnarBatchScan
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val enableColumnarHiveTableScan: Boolean = columnarConf.enableColumnarHiveTableScan
  val enableColumnarProject: Boolean = !scanOnly && columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarHashAgg: Boolean = !scanOnly && columnarConf.enableColumnarHashAgg
  val enableColumnarUnion: Boolean = !scanOnly && columnarConf.enableColumnarUnion
  val enableColumnarExpand: Boolean = !scanOnly && columnarConf.enableColumnarExpand
  val enableColumnarShuffledHashJoin: Boolean =
    !scanOnly && columnarConf.enableColumnarShuffledHashJoin
  val enableColumnarBroadcastExchange: Boolean = !scanOnly &&
    columnarConf.enableColumnarBroadcastExchange
  val enableColumnarBroadcastJoin: Boolean = !scanOnly &&
    columnarConf.enableColumnarBroadcastJoin
  val enableColumnarLimit: Boolean = !scanOnly && columnarConf.enableColumnarLimit
  val enableColumnarGenerate: Boolean = !scanOnly && columnarConf.enableColumnarGenerate
  val enableColumnarCoalesce: Boolean = !scanOnly && columnarConf.enableColumnarCoalesce
  val enableTakeOrderedAndProject: Boolean =
    !scanOnly && columnarConf.enableTakeOrderedAndProject &&
      enableColumnarSort && enableColumnarLimit && enableColumnarShuffle && enableColumnarProject
  val enableColumnarWrite: Boolean = BackendsApiManager.getSettings.enableNativeWriteFiles()
  val enableCartesianProduct: Boolean =
    BackendsApiManager.getSettings.supportCartesianProductExec() &&
      columnarConf.cartesianProductTransformerEnabled

  def apply(plan: SparkPlan): SparkPlan = {
    addFallbackTags(plan)
  }

  /** Inserts a transformable tag on top of those that are not supported. */
  private def addFallbackTags(plan: SparkPlan): SparkPlan = {
    // Walk the tree with post-order
    val out = plan.mapChildren(addFallbackTags)
    addFallbackTag(out)
    out
  }

  private def addFallbackTag(plan: SparkPlan): Unit = {
    if (FallbackHints.isTaggedFallback(plan)) {
      logDebug(
        s"Skip adding transformable tag, since plan already tagged as " +
          s"${FallbackHints.getHint(plan)}: ${plan.toString()}")
      return
    }
    try {
      plan match {
        case plan: BatchScanExec =>
          if (!enableColumnarBatchScan) {
            FallbackHints.tagFallback(plan, "columnar BatchScan is disabled")
          } else {
            // IF filter expressions aren't empty, we need to transform the inner operators.
            if (plan.runtimeFilters.isEmpty) {
              val transformer =
                ScanTransformerFactory
                  .createBatchScanTransformer(plan, validation = true)
                  .asInstanceOf[BatchScanExecTransformer]
              transformer.doValidate().tagOnFallback(plan)
            }
          }
        case plan: FileSourceScanExec =>
          if (!enableColumnarFileScan) {
            FallbackHints.tagFallback(
              plan,
              "columnar FileScan is not enabled in FileSourceScanExec")
          } else {
            // IF filter expressions aren't empty, we need to transform the inner operators.
            if (plan.partitionFilters.isEmpty) {
              val transformer =
                ScanTransformerFactory.createFileSourceScanTransformer(plan, validation = true)
              transformer.doValidate().tagOnFallback(plan)
            }
          }
        case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
          if (!enableColumnarHiveTableScan) {
            FallbackHints.tagFallback(plan, "columnar hive table scan is disabled")
          } else {
            HiveTableScanExecTransformer.validate(plan).tagOnFallback(plan)
          }
        case plan: ProjectExec =>
          if (!enableColumnarProject) {
            FallbackHints.tagFallback(plan, "columnar project is disabled")
          } else {
            val transformer = ProjectExecTransformer(plan.projectList, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: FilterExec =>
          val childIsScan = plan.child.isInstanceOf[FileSourceScanExec] ||
            plan.child.isInstanceOf[BatchScanExec]
          if (!enableColumnarFilter) {
            FallbackHints.tagFallback(plan, "columnar Filter is not enabled in FilterExec")
          } else if (scanOnly && !childIsScan) {
            // When scanOnly is enabled, filter after scan will be offloaded.
            FallbackHints.tagFallback(
              plan,
              "ScanOnly enabled and plan child is not Scan in FilterExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genFilterExecTransformer(plan.condition, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) {
            FallbackHints.tagFallback(
              plan,
              "columnar HashAggregate is not enabled in HashAggregateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child
              )
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: SortAggregateExec =>
          if (!BackendsApiManager.getSettings.replaceSortAggWithHashAgg) {
            FallbackHints.tagFallback(plan, "replaceSortAggWithHashAgg is not enabled")
          } else if (!enableColumnarHashAgg) {
            FallbackHints.tagFallback(plan, "columnar HashAgg is not enabled in SortAggregateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child
              )
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: ObjectHashAggregateExec =>
          if (!enableColumnarHashAgg) {
            FallbackHints.tagFallback(
              plan,
              "columnar HashAgg is not enabled in ObjectHashAggregateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child
              )
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: UnionExec =>
          if (!enableColumnarUnion) {
            FallbackHints.tagFallback(plan, "columnar Union is not enabled in UnionExec")
          } else {
            val transformer = ColumnarUnionExec(plan.children)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: ExpandExec =>
          if (!enableColumnarExpand) {
            FallbackHints.tagFallback(plan, "columnar Expand is not enabled in ExpandExec")
          } else {
            val transformer = ExpandExecTransformer(plan.projections, plan.output, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }

        case plan: WriteFilesExec =>
          if (!enableColumnarWrite || !BackendsApiManager.getSettings.supportTransformWriteFiles) {
            FallbackHints.tagFallback(plan, "columnar Write is not enabled in WriteFilesExec")
          } else {
            val transformer = WriteFilesExecTransformer(
              plan.child,
              plan.fileFormat,
              plan.partitionColumns,
              plan.bucketSpec,
              plan.options,
              plan.staticPartitions)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: SortExec =>
          if (!enableColumnarSort) {
            FallbackHints.tagFallback(plan, "columnar Sort is not enabled in SortExec")
          } else {
            val transformer =
              SortExecTransformer(plan.sortOrder, plan.global, plan.child, plan.testSpillFrequency)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: ShuffleExchangeExec =>
          if (!enableColumnarShuffle) {
            FallbackHints.tagFallback(
              plan,
              "columnar Shuffle is not enabled in ShuffleExchangeExec")
          } else {
            val transformer = ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              plan.child,
              plan.shuffleOrigin,
              plan.child.output)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: ShuffledHashJoinExec =>
          if (!enableColumnarShuffledHashJoin) {
            FallbackHints.tagFallback(
              plan,
              "columnar shufflehashjoin is not enabled in ShuffledHashJoinExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genShuffledHashJoinExecTransformer(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                plan.buildSide,
                plan.condition,
                plan.left,
                plan.right,
                plan.isSkewJoin)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: BroadcastExchangeExec =>
          // columnar broadcast is enabled only when columnar bhj is enabled.
          if (!enableColumnarBroadcastExchange) {
            FallbackHints.tagFallback(
              plan,
              "columnar BroadcastExchange is not enabled in BroadcastExchangeExec")
          } else {
            val transformer = ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case bhj: BroadcastHashJoinExec =>
          if (!enableColumnarBroadcastJoin) {
            FallbackHints.tagFallback(
              bhj,
              "columnar BroadcastJoin is not enabled in BroadcastHashJoinExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genBroadcastHashJoinExecTransformer(
                bhj.leftKeys,
                bhj.rightKeys,
                bhj.joinType,
                bhj.buildSide,
                bhj.condition,
                bhj.left,
                bhj.right,
                isNullAwareAntiJoin = bhj.isNullAwareAntiJoin)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin || plan.joinType == FullOuter) {
            FallbackHints.tagFallback(
              plan,
              "columnar sort merge join is not enabled or join type is FullOuter")
          } else {
            val transformer = SortMergeJoinExecTransformer(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              plan.condition,
              plan.left,
              plan.right,
              plan.isSkewJoin)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: CartesianProductExec =>
          if (!enableCartesianProduct) {
            FallbackHints.tagFallback(
              plan,
              "conversion to CartesianProductTransformer is not enabled.")
          } else {
            val transformer = CartesianProductExecTransformer(plan.left, plan.right, plan.condition)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: WindowExec =>
          if (!enableColumnarWindow) {
            FallbackHints.tagFallback(plan, "columnar window is not enabled in WindowExec")
          } else {
            val transformer = WindowExecTransformer(
              plan.windowExpression,
              plan.partitionSpec,
              plan.orderSpec,
              plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: CoalesceExec =>
          if (!enableColumnarCoalesce) {
            FallbackHints.tagFallback(plan, "columnar coalesce is not enabled in CoalesceExec")
          } else {
            val transformer = CoalesceExecTransformer(plan.numPartitions, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: GlobalLimitExec =>
          if (!enableColumnarLimit) {
            FallbackHints.tagFallback(plan, "columnar limit is not enabled in GlobalLimitExec")
          } else {
            val (limit, offset) =
              SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
            val transformer = LimitTransformer(plan.child, offset, limit)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: LocalLimitExec =>
          if (!enableColumnarLimit) {
            FallbackHints.tagFallback(plan, "columnar limit is not enabled in GlobalLimitExec")
          } else {
            val transformer = LimitTransformer(plan.child, 0L, plan.limit)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: GenerateExec =>
          if (!enableColumnarGenerate) {
            FallbackHints.tagFallback(plan, "columnar generate is not enabled in GenerateExec")
          } else {
            val transformer = GenerateExecTransformer(
              plan.generator,
              plan.requiredChildOutput,
              plan.outer,
              plan.generatorOutput,
              plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: EvalPythonExec =>
          val transformer = EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case _: AQEShuffleReadExec =>
        // Considered transformable by default.
        case plan: TakeOrderedAndProjectExec =>
          if (!enableTakeOrderedAndProject) {
            FallbackHints.tagFallback(
              plan,
              "columnar topK is not enabled in TakeOrderedAndProjectExec")
          } else {
            val (limit, offset) =
              SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
            val transformer = TakeOrderedAndProjectExecTransformer(
              limit,
              plan.sortOrder,
              plan.projectList,
              plan.child,
              offset)
            transformer.doValidate().tagOnFallback(plan)
          }
        case _ =>
        // Currently we assume a plan to be transformable by default.
      }
    } catch {
      case e: UnsupportedOperationException =>
        FallbackHints.tagFallback(
          plan,
          s"${e.getMessage}, original Spark plan is " +
            s"${plan.getClass}(${plan.children.toList.map(_.getClass)})")
    }
  }
}

case class RemoveFallbackHintRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach(FallbackHints.untag)
    plan
  }
}
