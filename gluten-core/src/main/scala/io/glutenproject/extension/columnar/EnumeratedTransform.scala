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

import io.glutenproject.cbo.PropertySet
import io.glutenproject.cbo.rule.{CboRule, Shape, Shapes}
import io.glutenproject.extension.columnar.MiscColumnarRules.TransformPreOverrides
import io.glutenproject.planner.GlutenOptimization
import io.glutenproject.planner.property.GlutenProperties
import io.glutenproject.utils.LogLevelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class EnumeratedTransform(session: SparkSession, outputsColumnar: Boolean)
  extends Rule[SparkPlan]
  with LogLevelUtil {
  import EnumeratedTransform._

  private val cboRules = Seq(SingleNodeTransformCboRule())
  private val optimization = GlutenOptimization(cboRules)

  private val reqConvention = GlutenProperties.Conventions.ANY
  private val altConventions =
    Seq(GlutenProperties.Conventions.GLUTEN_COLUMNAR, GlutenProperties.Conventions.ROW_BASED)

  override def apply(plan: SparkPlan): SparkPlan = {
    val reqPropSet = PropertySet(List(GlutenProperties.Schemas.ANY, reqConvention))
    val altPropSets =
      altConventions.map(altConv => PropertySet(List(GlutenProperties.Schemas.ANY, altConv)))
    val planner = optimization.newPlanner(plan, reqPropSet, altPropSets)
    val out = planner.plan()
    out
  }
}

object EnumeratedTransform {
  private case class SingleNodeTransformCboRule() extends CboRule[SparkPlan] {
    private val delegate = new TransformPreOverrides.ReplaceSingleNode()

    override def shift(node: SparkPlan): Iterable[SparkPlan] = {
      val out = List(delegate.replaceWithTransformerPlan(node))
      out
    }

    override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
  }
}
