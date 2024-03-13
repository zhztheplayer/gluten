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
package io.glutenproject.cbo

import io.glutenproject.cbo.rule.CboRule

import scala.collection.mutable

trait Optimization[T <: AnyRef] {
  def optimize(plan: T): T
}

/**
 * General-purpose cost-based optimization consisting of a set of sub-rules. Generic T is the type
 * of the plan node.
 */
class Cbo[T <: AnyRef] private (
    val config: CboConfig,
    val costModel: CostModel[T],
    val planModel: PlanModel[T],
    val propertyModel: PropertyModel[T],
    val explain: CboExplain[T],
    val ruleFactory: CboRule.Factory[T])
  extends Optimization[T] {
  import Cbo._

  def withNewConfig(confFunc: CboConfig => CboConfig): Cbo[T] = {
    new Cbo(confFunc(config), costModel, planModel, propertyModel, explain, ruleFactory)
  }

  // Normal groups start with ID 0, so it's safe to use -1 to do validation.
  private val dummyGroup: T = planModel.newGroupLeaf(-1, PropertySet(Seq.empty))
  private val infCost: Cost = costModel.makeInfCost()

  validateModels()

  private def assertThrows(u: => Unit): Unit = {
    var notThrew: Boolean = false
    try {
      u
      notThrew = true
    } catch {
      case _: Exception =>
    }
    assert(!notThrew)
  }

  private def validateModels(): Unit = {
    // Node groups are leafs.
    assert(planModel.childrenOf(dummyGroup) == List.empty)
    assertThrows {
      // Node groups don't have user-defined cost, expect exception here.
      costModel.costOf(dummyGroup)
    }
    propertyModel.propertyDefs.foreach {
      propDef =>
        // Node groups don't have user-defined property, expect exception here.
        assertThrows {
          propDef.getProperty(dummyGroup)
        }
    }
  }

  private val propSetFactory: PropertySetFactory[T] = PropertySetFactory(this)

  override def optimize(plan: T): T = {
    CboPlanner(this, plan).plan()
  }

  def newPlanner(plan: T): CboPlanner[T] = {
    CboPlanner(this, plan)
  }

  def newPlanner(plan: T, reqPropSet: PropertySet[T]): CboPlanner[T] = {
    CboPlanner(this, plan, reqPropSet)
  }

  private[cbo] def withNewChildren(node: T, newChildren: Seq[T]): T = {
    val oldChildren = planModel.childrenOf(node)
    assert(newChildren.size == oldChildren.size)
    val out = planModel.withNewChildren(node, newChildren)
    assert(planModel.childrenOf(out).size == newChildren.size)
    out
  }

  private[cbo] def isGroupLeaf(node: T): Boolean = {
    planModel.isGroupLeaf(node)
  }

  private[cbo] def isLeaf(node: T): Boolean = {
    planModel.childrenOf(node).isEmpty
  }

  private[cbo] def isCanonical(node: T): Boolean = {
    assert(!planModel.isGroupLeaf(node))
    planModel.childrenOf(node).forall(child => planModel.isGroupLeaf(child))
  }

  private[cbo] def getChildrenGroups(groups: Int => CboGroup[T], n: T): Seq[CboGroup[T]] = {
    assert(isCanonical(n))
    planModel
      .childrenOf(n)
      .map(child => groups(planModel.getGroupId(child)))
  }

  private[cbo] def propertySetFactory(): PropertySetFactory[T] = propSetFactory

  private[cbo] def dummyGroupLeaf(): T = {
    dummyGroup
  }

  private[cbo] def getInfCost(): Cost = infCost

  private[cbo] def isInfCost(cost: Cost) = costModel.costComparator().equiv(cost, infCost)
}

object Cbo {
  def apply[T <: AnyRef](
      costModel: CostModel[T],
      planModel: PlanModel[T],
      propertyModel: PropertyModel[T],
      explain: CboExplain[T],
      ruleFactory: CboRule.Factory[T]): Cbo[T] = {
    new Cbo[T](CboConfig(), costModel, planModel, propertyModel, explain, ruleFactory)
  }

  trait PropertySetFactory[T <: AnyRef] {
    def get(node: T): PropertySet[T]
    def childrenRequirements(node: T): Seq[PropertySet[T]]
  }

  private object PropertySetFactory {
    def apply[T <: AnyRef](cbo: Cbo[T]): PropertySetFactory[T] = new PropertySetFactoryImpl[T](cbo)

    private class PropertySetFactoryImpl[T <: AnyRef](val cbo: Cbo[T])
      extends PropertySetFactory[T] {
      private val propDefs: Seq[PropertyDef[T, _ <: Property[T]]] = cbo.propertyModel.propertyDefs

      override def get(node: T): PropertySet[T] = {
        val m: Map[PropertyDef[T, _ <: Property[T]], Property[T]] =
          propDefs.map(propDef => (propDef, propDef.getProperty(node))).toMap
        PropertySet[T](m)
      }

      override def childrenRequirements(node: T): Seq[PropertySet[T]] = {
        val builder: Seq[mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]] =
          cbo.planModel
            .childrenOf(node)
            .map(_ => mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]())

        propDefs
          .foldLeft(builder) {
            (
                builder: Seq[mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]],
                propDef: PropertyDef[T, _ <: Property[T]]) =>
              val childrenRequirements = propDef.getChildrenPropertyRequirements(node)
              builder.zip(childrenRequirements).map {
                case (childBuilder, childRequirement) =>
                  childBuilder += (propDef -> childRequirement)
              }
          }
          .map {
            builder: mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]] =>
              PropertySet[T](builder.toMap)
          }
      }
    }
  }
}
