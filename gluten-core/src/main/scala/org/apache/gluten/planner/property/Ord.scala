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

package org.apache.gluten.planner.property

import org.apache.gluten.ras.{Property, PropertyDef}

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.SparkPlan

sealed trait Ord extends Property[SparkPlan] {
  import Ord._
  override def definition(): PropertyDef[SparkPlan, _ <: Property[SparkPlan]] = {
    OrdDef
  }

  override def satisfies(other: Property[SparkPlan]): Boolean = {
    val req = other.asInstanceOf[Req]
    if (req.isAny) {
      return true
    }
    val prop = this.asInstanceOf[Prop]
    val out = SortOrder.orderingSatisfies(prop.prop, req.req)
    out
  }
}

object Ord {
  val any: Ord = Req(Nil)

  case class Prop(prop: Seq[SortOrder]) extends Ord
  case class Req(req: Seq[SortOrder]) extends Ord {
    def isAny: Boolean = {
      this == any
    }
  }
}

object OrdDef extends PropertyDef[SparkPlan, Ord] {
  override def any(): Ord = Ord.any
  override def getProperty(plan: SparkPlan): Ord = Ord.Prop(plan.outputOrdering)
  override def getChildrenConstraints(
      constraint: Property[SparkPlan],
      plan: SparkPlan): Seq[Ord] = {
    // TODO: Propagate constraints?
    plan.requiredChildOrdering.map(Ord.Req)
  }
}
