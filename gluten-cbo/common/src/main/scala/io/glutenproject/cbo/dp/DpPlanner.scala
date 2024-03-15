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
import io.glutenproject.cbo.rule.{EnforcerRuleSet, RuleApplier}

// TODO: Branch and bound pruning.
private class DpPlanner[T <: AnyRef] private (cbo: Cbo[T], plan: T, reqPropSet: PropertySet[T])
  extends CboPlanner[T] {
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
    BestFinder
      .unsafe(cbo, memoState, ???)
      .bestOf(groupId)
  }
}

object DpPlanner {
  def apply[T <: AnyRef](cbo: Cbo[T], plan: T, reqPropSet: PropertySet[T]): CboPlanner[T] = {
    new DpPlanner(cbo, plan, reqPropSet)
  }

  private class ExploreAdjustment[T <: AnyRef](
      cbo: Cbo[T],
      memoState: UnsafeMemoState[T],
      rules: Seq[RuleApplier[T]],
      enforcerRuleSet: EnforcerRuleSet[T])
    extends DpClusterAlgo.Adjustment[T] {

    override def beforeXSolved(x: CanonicalNode[T]): Unit = ???

    override def beforeYSolved(y: CboCluster[T]): Unit = ???
  }
}
