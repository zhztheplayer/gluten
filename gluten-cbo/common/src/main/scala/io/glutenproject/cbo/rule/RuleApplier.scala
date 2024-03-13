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

package io.glutenproject.cbo.rule

import io.glutenproject.cbo.{Cbo, Closure, EnforcerRuleFactory, Property, PropertyDef, PropertySet}
import io.glutenproject.cbo.path.CboPath
import io.glutenproject.cbo.util.NodeMap

import scala.collection.mutable

trait RuleApplier[T <: AnyRef] {
  def apply(path: CboPath[T]): Unit
  def shape(): Shape[T]
}

object RuleApplier {
  def apply[T <: AnyRef](cbo: Cbo[T], closure: Closure[T], rule: CboRule[T]): RuleApplier[T] = {
    new ShapeAwareRuleApplier(cbo, closure, rule)
  }

  private class ShapeAwareRuleApplier[T <: AnyRef](
      cbo: Cbo[T],
      closure: Closure[T],
      rule: CboRule[T])
    extends RuleApplier[T] {
    private val ruleShape = rule.shape()
    private val cache = new NodeMap[T, T](cbo)

    def apply(path: CboPath[T]): Unit = {
      if (!ruleShape.identity(path)) {
        return
      }
      val in = path.plan()
      if (cache.contains(in)) {
        return
      }
      val equivalents = rule.shift(in)
      cache.put(in, in)
      apply0(path, equivalents)
    }

    private def apply0(path: CboPath[T], equivalents: Iterable[T]): Unit = {
      equivalents.foreach(equiv => closure.defineEquiv(path.node().self().asCanonical(), equiv))
    }

    override def shape(): Shape[T] = ruleShape
  }
}

trait EnforcerRuleSet[T <: AnyRef] {
  def rulesOf(reqPropSet: PropertySet[T]): Seq[RuleApplier[T]]
}

object EnforcerRuleSet {
  def apply[T <: AnyRef](cbo: Cbo[T], closure: Closure[T]): EnforcerRuleSet[T] = {
    new EnforcerRuleSetImpl(cbo, closure)
  }

  private case class EnforcerRuleDecorator[T <: AnyRef](
      cbo: Cbo[T],
      reqProp: Property[T],
      rule: CboRule[T])
    extends CboRule[T] {
    private val propDef = reqProp.definition()

    override def shift(node: T): Iterable[T] = {
      assert(!cbo.isGroupLeaf(node))
      val selfProp = propDef.getProperty(node)
      if (selfProp.satisfies(reqProp)) {
        return List.empty
      }
      val outs = rule.shift(node)
      outs.foreach {
        out =>
          val outProp = propDef.getProperty(out)
          assert(outProp.satisfies(reqProp))
      }
      outs
    }

    override def shape(): Shape[T] = rule.shape()
  }

  private class EnforcerRuleSetImpl[T <: AnyRef](cbo: Cbo[T], closure: Closure[T])
    extends EnforcerRuleSet[T] {
    private val factoryBuffer =
      mutable.Map[PropertyDef[T, _ <: Property[T]], EnforcerRuleFactory[T]]()
    private val buffer = mutable.Map[Property[T], Seq[RuleApplier[T]]]()

    override def rulesOf(reqPropSet: PropertySet[T]): Seq[RuleApplier[T]] = {
      reqPropSet.getMap
        .map {
          case (reqPropDef, reqProp) =>
            buffer.getOrElseUpdate(
              reqProp, {
                val factory = factoryBuffer.getOrElseUpdate(
                  reqPropDef,
                  cbo.propertyModel.newEnforcerRuleFactory(reqPropDef))
                factory
                  .newEnforcerRules(reqProp)
                  .map(rule => RuleApplier(cbo, closure, EnforcerRuleDecorator(cbo, reqProp, rule)))
              }
            )
        }
        .toSeq
        .flatten
    }
  }
}
