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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.RuleApi
import org.apache.gluten.datasource.ArrowConvertorRule
import org.apache.gluten.extension.{ArrowScanReplaceRule, BloomFilterMightContainJointRewriteRule, CollectRewriteRule, FlushableHashAggregateRule, HLLRewriteRule, RuleInjector}
import org.apache.gluten.extension.columnar.{AddFallbackTagRule, CollapseProjectExecTransformer, EliminateLocalSort, EnsureLocalSortRequirements, ExpandFallbackPolicy, FallbackEmptySchemaRelation, FallbackMultiCodegens, FallbackOnANSIMode, MergeTwoPhasesHashBaseAggregate, PlanOneRowRelation, RemoveFallbackTagRule, RemoveNativeWriteFilesSortAndProject, RewriteTransformer}
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast, TransformPreOverrides}
import org.apache.gluten.extension.columnar.enumerated.EnumeratedTransform
import org.apache.gluten.extension.columnar.rewrite.RewriteSparkPlanRulesManager
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, GlutenFallbackReporter}
import org.apache.spark.sql.expression.UDFResolver
import org.apache.spark.util.SparkPlanRules

class VeloxRuleApi extends RuleApi {
  import VeloxRuleApi._

  override def injectRules(injector: RuleInjector): Unit = {
    injectSpark(injector.spark)
    injectGluten(injector.gluten)
    injectRas(injector.ras)
  }
}

private object VeloxRuleApi {
  def injectSpark(injector: RuleInjector.SparkInjector): Unit = {
    // Regular Spark rules.
    injector.injectOptimizerRule(CollectRewriteRule.apply)
    injector.injectOptimizerRule(HLLRewriteRule.apply)
    UDFResolver.getFunctionSignatures.foreach(injector.injectFunction)
    injector.injectPostHocResolutionRule(ArrowConvertorRule.apply)
  }

  def injectGluten(injector: RuleInjector.GlutenInjector): Unit = {
    // Gluten columnar: Transform rules.
    injector.injectTransform(_ => RemoveTransitions)
    injector.injectTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectTransform(c => PlanOneRowRelation.apply(c.session))
    injector.injectTransform(_ => RewriteSubqueryBroadcast())
    injector.injectTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
    injector.injectTransform(c => ArrowScanReplaceRule.apply(c.session))
    injector.injectTransform(_ => FallbackEmptySchemaRelation())
    injector.injectTransform(c => MergeTwoPhasesHashBaseAggregate.apply(c.session))
    injector.injectTransform(_ => RewriteSparkPlanRulesManager())
    injector.injectTransform(_ => AddFallbackTagRule())
    injector.injectTransform(_ => TransformPreOverrides())
    injector.injectTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectTransform(c => RewriteTransformer.apply(c.session))
    injector.injectTransform(_ => EnsureLocalSortRequirements)
    injector.injectTransform(_ => EliminateLocalSort)
    injector.injectTransform(_ => CollapseProjectExecTransformer)
    if (GlutenConfig.getConf.enableVeloxFlushablePartialAggregation) {
      injector.injectTransform(c => FlushableHashAggregateRule.apply(c.session))
    }
    SparkPlanRules
      .extendedColumnarRules(GlutenConfig.getConf.extendedColumnarTransformRules)
      .foreach(each => injector.injectTransform(c => each(c.session)))
    injector.injectTransform(c => InsertTransitions(c.outputsColumnar))

    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(
      c => ExpandFallbackPolicy(c.ac.isAdaptiveContext(), c.ac.originalPlan()))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => each(c.session)))
    injector.injectPost(_ => ColumnarCollapseTransformStages(GlutenConfig.getConf))
    SparkPlanRules
      .extendedColumnarRules(GlutenConfig.getConf.extendedColumnarPostRules)
      .foreach(each => injector.injectTransform(c => each(c.session)))

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(c => GlutenFallbackReporter(GlutenConfig.getConf, c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }

  def injectRas(injector: RuleInjector.RasInjector): Unit = {
    // Gluten RAS: Pre rules.
    injector.inject(_ => RemoveTransitions)
    injector.inject(c => FallbackOnANSIMode.apply(c.session))
    injector.inject(c => PlanOneRowRelation.apply(c.session))
    injector.inject(_ => FallbackEmptySchemaRelation())
    injector.inject(_ => RewriteSubqueryBroadcast())
    injector.inject(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
    injector.inject(c => ArrowScanReplaceRule.apply(c.session))
    injector.inject(c => MergeTwoPhasesHashBaseAggregate.apply(c.session))

    // Gluten RAS: The RAS rule.
    injector.inject(c => EnumeratedTransform(c.session, c.outputsColumnar))

    // Gluten RAS: Post rules.
    injector.inject(_ => RemoveTransitions)
    injector.inject(_ => RemoveNativeWriteFilesSortAndProject())
    injector.inject(c => RewriteTransformer.apply(c.session))
    injector.inject(_ => EnsureLocalSortRequirements)
    injector.inject(_ => EliminateLocalSort)
    injector.inject(_ => CollapseProjectExecTransformer)
    if (GlutenConfig.getConf.enableVeloxFlushablePartialAggregation) {
      injector.inject(c => FlushableHashAggregateRule.apply(c.session))
    }
    SparkPlanRules
      .extendedColumnarRules(GlutenConfig.getConf.extendedColumnarTransformRules)
      .foreach(each => injector.inject(c => each(c.session)))
    injector.inject(c => InsertTransitions(c.outputsColumnar))
    injector.inject(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.inject(c => each(c.session)))
    injector.inject(_ => ColumnarCollapseTransformStages(GlutenConfig.getConf))
    SparkPlanRules
      .extendedColumnarRules(GlutenConfig.getConf.extendedColumnarPostRules)
      .foreach(each => injector.inject(c => each(c.session)))
    injector.inject(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.inject(c => GlutenFallbackReporter(GlutenConfig.getConf, c.session))
    injector.inject(_ => RemoveFallbackTagRule())
  }
}