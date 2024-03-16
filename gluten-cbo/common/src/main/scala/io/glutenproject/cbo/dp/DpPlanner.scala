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

package io.glutenproject.cbo.dp

import io.glutenproject.cbo._
import io.glutenproject.cbo.Best.KnownCostPath
import io.glutenproject.cbo.best.BestFinder
import io.glutenproject.cbo.path.{CboPath, PathFinder}
import io.glutenproject.cbo.rule.{EnforcerRuleSet, RuleApplier, Shape}

// TODO: Branch and bound pruning.
private class DpPlanner[T <: AnyRef] private (cbo: Cbo[T], plan: T, reqPropSet: PropertySet[T])
  extends CboPlanner[T] {
  import DpPlanner._

  private val memo = Memo.unsafe(cbo)
  private val rules = cbo.ruleFactory.create().map(rule => RuleApplier(cbo, memo, rule))
  private val enforcerRuleSet = EnforcerRuleSet[T](cbo, memo)

  private lazy val rootGroupId: Int = {
    memo.memorize(plan, reqPropSet).id()
  }

  private lazy val best: (Best[T], KnownCostPath[T]) = {
    val groupId = rootGroupId
    val memoState = memo.newUnsafeState()
    val best = findBest(memoState, groupId)
    (best, best.path(memoState.allGroups()))
  }

  override def plan(): T = {
    best._2.cboPath.plan()
  }

  override def newState(): PlannerState[T] =
    PlannerState(cbo, memo.newState(), rootGroupId, best._1)

  private def findBest(memoState: UnsafeMemoState[T], groupId: Int): Best[T] = {
    val cluster = memoState.allClusters()(memoState.allGroups()(groupId).clusterId())
    val algoDef = new DpExploreAlgoDef[T]
    val adjustment = new ExploreAdjustment(cbo, memoState, rules, enforcerRuleSet)
    val conf = DpZipperAlgo.Conf(
      solveYOnlyAllXsSolved = true,
      solveXOnlyAllYsSolved = true,
      excludeCyclesOnX = false,
      excludeCyclesOnY = true)
    DpClusterAlgo.resolve(memoState, algoDef, conf, adjustment, cluster)
    val finder = BestFinder(cbo, memoState.toSafe())
    finder.bestOf(groupId)
  }
}

object DpPlanner {
  def apply[T <: AnyRef](cbo: Cbo[T], plan: T, reqPropSet: PropertySet[T]): CboPlanner[T] = {
    new DpPlanner(cbo, plan, reqPropSet)
  }

  private class DpExploreAlgoDef[T <: AnyRef] extends DpClusterAlgoDef[T, AnyRef, AnyRef] {
    private val flag = new AnyRef()
    override def solveNode(
        node: CanonicalNode[T],
        childrenClustersOutput: CboCluster[T] => Option[AnyRef]): Option[AnyRef] = {
      Some(flag)
    }
    override def solveCluster(
        group: CboCluster[T],
        nodesOutput: CanonicalNode[T] => Option[AnyRef]): Option[AnyRef] = {
      Some(flag)
    }
  }

  private class ExploreAdjustment[T <: AnyRef](
      cbo: Cbo[T],
      memoState: UnsafeMemoState[T],
      rules: Seq[RuleApplier[T]],
      enforcerRuleSet: EnforcerRuleSet[T])
    extends DpClusterAlgo.Adjustment[T] {
    private val allGroups = memoState.allGroups()
    private val allClusters = memoState.allClusters()
    override def beforeXSolved(can: CanonicalNode[T]): Unit = {
      if (rules.isEmpty) {
        return
      }
      val shapes = rules.map(_.shape())
      findPaths(can, shapes)(path => rules.foreach(rule => applyRule(rule, path)))
    }

    override def beforeYSolved(cluster: CboCluster[T]): Unit = {
      cluster.groups().foreach {
        group =>
          val reqPropSet = group.propSet()
          val enforcerRules = enforcerRuleSet.rulesOf(reqPropSet)
          if (enforcerRules.nonEmpty) {
            val shapes = enforcerRules.map(_.shape())
            allClusters(group.clusterId()).nodes().foreach {
              node =>
                findPaths(node, shapes)(
                  path => enforcerRules.foreach(rule => applyRule(rule, path)))
            }
          }
      }
    }

    private def findPaths(canonical: CanonicalNode[T], shapes: Seq[Shape[T]])(
        onFound: CboPath[T] => Unit): Unit = {
      val finder = shapes
        .foldLeft(
          PathFinder
            .builder(cbo, allGroups)) {
          case (builder, shape) =>
            builder.output(shape.wizard())
        }
        .build()
      finder.find(canonical).foreach(path => onFound(path))
    }

    private def applyRule(rule: RuleApplier[T], path: CboPath[T]): Unit = {
      rule.apply(path)
    }
  }
}
